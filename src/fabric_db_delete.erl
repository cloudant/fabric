% Copyright 2010 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric_db_delete).
-export([go/2]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").

%% @doc Options aren't used at all now in couch on delete but are left here
%%      to be consistent with fabric_db_create for possible future use
%% @see couch_server:delete_db
%%
go(DbName, Options) ->
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, delete_db, [DbName]),
    Acc0 = fabric_dict:init(Workers, nil),

    RexiMon = fabric_util:create_monitors(Shards),

    W = list_to_integer(couch_util:get_value(w,Options,"2")),

    Result =
        case fabric_util:recv(Workers, #shard.ref, fun handle_message/3, {length(Workers), W, Acc0}) of
        {ok, ok} ->
            ok;
        {ok, part_ok} ->
            part_ok;
        {ok, not_found} ->
            erlang:error(database_does_not_exist, DbName);
        Error ->
            Error
        end,
    rexi_monitor:stop(RexiMon),
    Result.

handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Shard, {WorkerLen, W, Counters}) ->
    NewCounters =
        fabric_dict:filter(fun(#shard{node=Node}, _) ->
                                Node =/= NodeRef
                       end, Counters),
    maybe_answer(WorkerLen - (fabric_dict:size(Counters) - fabric_dict:size(NewCounters)),
          W, NewCounters);

handle_message({rexi_EXIT, Reason}, _Worker, _Counters) ->
    {error, Reason};

handle_message(Msg, Shard, {WorkerLen, W, Counters}) ->
    C1 = fabric_dict:store(Shard, Msg, Counters),
    maybe_answer(WorkerLen-1, W, C1).

maybe_answer(WorkerLen, W, Counters) ->
    case fabric_view:is_progress_possible(Counters) of
    true ->
        case lists:keymember(not_found, 2, Counters) of
        true ->
            {stop, not_found};
        false ->
            QuorumMet = fabric_util:quorum_met(W, Counters),
            case QuorumMet of
            true ->
                {stop, ok};
            false ->
                if WorkerLen > 0 ->
                    {ok, {WorkerLen, W, Counters}};
                   true ->
                    {stop, part_ok}
                end
            end
        end;
    false ->
        {error, internal_server_error}
    end.
