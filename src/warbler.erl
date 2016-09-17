-module(warbler).
-export([init/0,get_incidents/2,bucket/1,get_incident_keys/1]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


init()->
    lists:map(fun(X)->
		      application:start(X)
	      end, [crypto, public_key, ssl, inets]).

get_incidents(Token, TeamId)->
    get_incidents(Token, TeamId, []).

get_incidents(Token, TeamId, Acc)->
   {ok,{{"HTTP/1.1",200,"OK"}, _, Body}}= httpc:request(get,
		  {lists:flatten(["https://api.pagerduty.com/incidents?date_range=all&team_ids%5B%5D=",
				  TeamId,
				  "&offset=",
				  integer_to_list(length(Acc)),
				  "&time_zone=UTC"]),
		   [{"Accept", "application/vnd.pagerduty+json;version=2"},
		     {"Host","api.pagerduty.com"},
		    {"Authorization",
		     lists:append("Token token=", Token)
		    }
		   ]
		  }, [], [{headers_as_is, true}]),
    DecodedBody = jiffy:decode(Body),
    {Response} = DecodedBody,
    {<<"incidents">>, Incidents}=lists:keyfind(<<"incidents">>, 1, Response),
    NewAcc = lists:append(Acc, Incidents),
    {<<"more">>,More} = lists:keyfind(<<"more">>, 1, Response),
    io:format("~p~n", [More]),
    case More of
	true ->
	    io:fwrite("~p~n", [integer_to_list(length(NewAcc))]),
	    get_incidents(Token, TeamId, NewAcc);
	_ -> NewAcc
    end.
		
bucket(Incidents)->
    bucket(Incidents, []).

bucket([{Incident}|Incidents], Buckets)->

    {<<"created_at">>, CreatedAt}=lists:keyfind(<<"created_at">>, 1, Incident),
    {<<"incident_key">>,IncidentKey}=lists:keyfind(<<"incident_key">>, 1, Incident),

    CreatedAtStr = binary_to_list(CreatedAt),
    IncidentKeyStr = binary_to_list(IncidentKey),

    [Year | [Month | [Day | _]]]= string:tokens(CreatedAtStr, "-T"),
    Date = {list_to_integer(Year), list_to_integer(Month), list_to_integer(Day)},
    IsoWeek = calendar:iso_week_number(Date),
    Entry = lists:keyfind(IsoWeek, 1, Buckets),

    case Entry of 
	false ->
	    NewEntry = {IsoWeek, [{IncidentKeyStr, 1}]}, 
	    NewBuckets = lists:append(Buckets, [NewEntry]);
	{IsoWeek, IncidentCounts} ->
	    case lists:keyfind(IncidentKeyStr, 1, IncidentCounts) of
		false ->
		    NewIncidentCounts = lists:append(IncidentCounts, [{IncidentKeyStr, 1}]);
		{IncidentKeyStr, IncidentCount} ->
		    NewIncidentCounts = lists:keyreplace(IncidentKeyStr, 1, IncidentCounts, {IncidentKeyStr, IncidentCount + 1})
	    end,
	    NewBuckets = lists:keyreplace(IsoWeek, 1, Buckets, {IsoWeek, NewIncidentCounts})
    end,
    bucket(Incidents, NewBuckets);
bucket([], Buckets) ->
    Buckets.

		    
get_incident_keys(Buckets)-> 
    get_incident_keys(Buckets, []).   

get_incident_keys([H|T], Acc)->
    {_,Incidents}=H,
    NewAcc = lists:append(Acc, [Incident || {Incident, _} <- Incidents ]),
    get_incident_keys(T, NewAcc);
get_incident_keys([], Acc)->
    lists:sort(sets:to_list(sets:from_list(Acc))).
    
get_weeks(Buckets)->    
    lists:sort([Week || {Week,_} <- Buckets]).
    
create_table(Buckets)->    
    create_table(Buckets, get_weeks(Buckets), get_incident_keys(Buckets), [[] || _ <- get_incident_keys(Buckets)]).    
create_table(Buckets, [Week|T], Keys, Acc)->    
    {Week, Incidents} = lists:keyfind(Week, 1, Buckets),
    Column = lists:map(fun(X)->
		      case lists:keyfind(X, 1, Incidents) of
			  false ->
			      0;
			  {X,Count} ->
			      Count
		      end
	      end, Keys),
    NewAcc = lists:zipwith(fun(X,Y) -> lists:append(X, [Y]) end, Acc, Column), 
    create_table(Buckets, T, Keys,NewAcc);
create_table(Buckets, [], Keys, NewAcc)->
    Weeks = get_weeks(Buckets),
    Headers=lists:append([undefined], Weeks),
    Body=lists:zipwith(fun(X, Y)->lists:append([Y], X) end, NewAcc, Keys),
    lists:append([Headers], Body).

add_column(Table, Column)->
    add_column(Table,Column,[]).
add_column([Row|Table],[NewValue|Column],Acc)->
    NewRow= lists:append(Row, NewValue), 
    NewAcc=lists:append(Acc, [NewRow]),
    add_column(Table, Column, NewAcc);
add_column([],[],Acc) ->
    Acc.

table_to_csv(Table)->    
    ok.

    
    


    
    
	    

-ifdef(TEST).

bucket_test()->
    Incident001 = {[{<<"created_at">>, <<"2015-12-28T16:42:44Z">>},
		    {<<"incident_key">>, <<"my_incident_key">>}
		   ]},

    Incident002 = {[{<<"created_at">>, <<"2015-12-28T22:41:01Z">>},
		    {<<"incident_key">>, <<"my_incident_key">>}
		   ]},
    


    Incidents001 = [Incident001],
    Incidents002 = [Incident001, Incident002],
    
    Buckets001 = warbler:bucket(Incidents001),
    Buckets002 = warbler:bucket(Incidents002),
    
    
    {{2015,53},[{"my_incident_key", 1}]} = lists:keyfind({2015,53}, 1, Buckets001),
    {{2015,53},[{"my_incident_key", 2}]} = lists:keyfind({2015,53}, 1, Buckets002). 


get_incident_keys_test() ->
    Buckets = [
     {{2015,53},[{"bat_incident_key", 1},{"bar_incident_key", 4}, {"foo_incident_key", 1}]},
     {{2015,51},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
     {{2015,50},[{"baz_incident_key", 5},{"bar_incident_key", 1}]}
    ],
    %% incident keys should be returned sorted
    ["bar_incident_key", "bat_incident_key", "baz_incident_key", "bing_incident_key", "foo_incident_key"] = get_incident_keys(Buckets).

get_weeks_test() ->
    Buckets = [
	       {{2015,53},[{"bat_incident_key", 1},{"bar_incident_key", 4}, {"foo_incident_key", 1}]},
	       {{2015,51},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2016,1},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2016,2},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2016,24},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2015,1},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2015,50},[{"baz_incident_key", 5},{"bar_incident_key", 1}]}
	      ],

    OrderedWeeks = [{2015,1},
		    {2015,50},
		    {2015,51},
		    {2015,53},
		    {2016,1},
		    {2016,2},
		    {2016,24}
		   ],
    OrderedWeeks = get_weeks(Buckets).
    
create_table_test()->
    Buckets = [
	       {{2015,53},[{"bat_incident_key", 1},{"bar_incident_key", 4}, {"foo_incident_key", 1}]},
	       {{2015,51},[{"bat_incident_key", 3},{"bar_incident_key", 1}, {"bing_incident_key", 1}]},
	       {{2015,50},[{"baz_incident_key", 5},{"bar_incident_key", 1}]}
	      ],
    Table = [
	     [undefined, {2015,50}, {2015,51}, {2015,53}],
	     ["bar_incident_key",1,1,4],
	     ["bat_incident_key",0,3,1],
	     ["baz_incident_key",5,0,0],
	     ["bing_incident_key",0,1,0],
	     ["foo_incident_key",0,0,1]
	    ],
    Table = create_table(Buckets).
    

table_to_csv_test()->
    Table = [
	     [undefined, {2015,50}, {2015,51}, {2015,53}],
	     ["bar_incident_key",1,1,4],
	     ["bat_incident_key",0,3,1],
	     ["baz_incident_key",5,0,0],
	     ["bing_incident_key",0,1,0],
	     ["foo_incident_key",0,0,1]
	    ],
    Csv=",2015-50,2015-51,2015-53\nbar_incident_key,1,1,4\nbat_incident_key,0,3,1\nbaz_incident_key,5,0,0\nbing_incident_key,0,1,0\nfoo_incident_key,0,0,1",
    Csv = table_to_csv(Table).

%add_column_test()->
%    Table=[[1,2],[4,5],[7,8]],
%    Column = [3,6,9],
%    [[1,2,3],[4,5,6],[7,8,9]]=add_column(Table, Column).

-endif.
