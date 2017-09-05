-module(warbler).
-export([init/0,get_incidents/2,bucket/1,bucket/2,get_incident_keys/1,create_table/1,table_to_csv/1,log_incident_ids/2, get_service/2]).


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
    Parameters = [{"date_range", "all"},
		 {"team_ids%5B%5D", TeamId},
		 {"offset", integer_to_list(length(Acc))}],
    DecodedBody = pager_duty_request(Token, "incidents", Parameters),
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

get_service(Token, ServiceId)->
    Parameters = [{"include%5B%5D", "teams"}],
    DecodedBody = pager_duty_request(Token, lists:flatten(["services", "/", ServiceId]), Parameters),
    DecodedBody.






pager_duty_request(Token, Endpoint, Parameters)->
    Url = "https://api.pagerduty.com",
    ParameterString = create_parameter_string(Parameters),
    RequestUrl=lists:flatten([Url, "/", Endpoint, "?", ParameterString]),    
    io:format("~p~n", [RequestUrl]),
     {ok,{{"HTTP/1.1",200,"OK"}, _, Body}}= httpc:request(get,
		  {RequestUrl,
		   [{"Accept", "application/vnd.pagerduty+json;version=2"},
		     {"Host","api.pagerduty.com"},
		    {"Authorization",
		     lists:append("Token token=", Token)
		    }
		   ]
		  }, [], [{headers_as_is, true}]),
    io:format("~p", [Body]),
    DecodedBody = jiffy:decode(Body),
    DecodedBody.




create_parameter_string([H|T])->
    {Key, Value}=H,
    Acc=lists:flatten([Key, "=", Value]),
    create_parameter_string(T, Acc).

create_parameter_string([H|T], Acc) ->
    {Key, Value}=H,
    NewAcc=lists:flatten([Acc, "&", Key, "=", Value]),
    create_parameter_string(T, NewAcc);
create_parameter_string([], Acc) ->
    Acc.





log_incident_ids(Incidents, File)->		
    {ok, Out} = file:open("/tmp/pgIncidents", [write]),
    F=fun(X)->
	      {Y}=X,
	      {_,Number}=lists:keyfind(<<"incident_number">>, 1,Y),
	      Number
      end,
    Numbers = lists:map(F, Incidents),
    lists:map(fun(X)->
		      io:format(Out, "~p~n", [X]) end,
	      Numbers).

% buckets incidents by incident key and the ISO week-of-year number in which it occurred.
bucket(Incidents)->
    bucket(Incidents, [], fun(X)->X end).

bucket(Incidents, KeyMapFunction)->
    bucket(Incidents, [], KeyMapFunction).

bucket([{Incident}|Incidents], Buckets, KeyMapFunction)->

    {<<"created_at">>, CreatedAt}=lists:keyfind(<<"created_at">>, 1, Incident),
    {<<"incident_key">>,IncidentKey}=lists:keyfind(<<"incident_key">>, 1, Incident),

    CreatedAtStr = binary_to_list(CreatedAt),
    IncidentKeyStr = KeyMapFunction(strip_timestamp(binary_to_list(IncidentKey))),


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
    bucket(Incidents, NewBuckets, KeyMapFunction);
bucket([], Buckets, _) ->
    Buckets.



strip_timestamp(IncidentKey)->
    ReplacedIncidentKey = re:replace(IncidentKey, "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z", "_"),
    
     
    lists:flatten(lists:map(fun(X)-> case is_binary(X) of
					 true ->
					     binary_to_list(X);
					 false ->
					     X
				     end
			    end, ReplacedIncidentKey)). 

		    
get_incident_keys(Buckets)-> 
    get_incident_keys(Buckets, []).   

get_incident_keys([H|T], Acc)->
    {_,Incidents}=H,
    NewAcc = lists:append(Acc, [strip_comma(Incident) || {Incident, _} <- Incidents ]),
    get_incident_keys(T, NewAcc);
get_incident_keys([], Acc)->
    lists:sort(sets:to_list(sets:from_list(Acc))).

strip_comma(String)->
    lists:flatten(string:tokens(String, ",")).
    
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

table_to_csv([Header|Body])->
    [_|Dates]=Header,
    DateStrings = [lists:flatten([integer_to_list(Year), "-", integer_to_list(Week)]) || {Year, Week} <- Dates],
    DateStringsHeader = lists:append([[""], DateStrings]),
    BodyStrings = lists:map(fun(X)-> [H|T] = X, TStrings = [integer_to_list(Y) || Y <- T], [H|TStrings] end, Body),
    Table = [DateStringsHeader|BodyStrings],
    CsvList = lists:map(fun(X) -> [H|T]=X, lists:flatten([H, [lists:flatten([",", Y]) || Y <- T], "\n"])  end, Table),
    lists:flatten(CsvList).





    
    
	    

-ifdef(TEST).

param_builder_test() ->
    ExpectedString = "date_range=all&include%5B%5D=teams&offset=100&time_zone=UTC",
    Parameters=[{"date_range", "all"},{"include%5B%5D", "teams"}, {"offset", "100"}, {"time_zone", "UTC"}],
    ?assert(create_parameter_string(Parameters)  =:= ExpectedString).

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
    ExpectedCsv=",2015-50,2015-51,2015-53\nbar_incident_key,1,1,4\nbat_incident_key,0,3,1\nbaz_incident_key,5,0,0\nbing_incident_key,0,1,0\nfoo_incident_key,0,0,1\n",
    Csv = table_to_csv(Table),
    ExpectedCsv = Csv.


-endif.
