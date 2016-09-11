-module(warbler).
-export([init/0,get_incidents/2,bucket/1]).


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
   {ok,{{"HTTP/1.1",200,"OK"}, Headers, Body}}= httpc:request(get,
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
    
    
    {{2015,53},[{"my_incident_key", 1}]} = lists:keyfind({2015,53}, 1, Buckets001). 

-endif.
