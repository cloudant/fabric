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

-module(fabric_db_doc_count).

-export([go/1]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(DbName) ->
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, get_doc_count, []),
    RexiMon = fabric_util:create_monitors(Shards),
    Acc0 = {length(Workers), fabric_dict:init(Workers, nil), 0},
    try
        fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0)
    after
        rexi_monitor:stop(RexiMon)
    end.

handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Shard, {WorkerLen, Counters, Acc}) ->
    NewCounters =
        fabric_dict:filter(fun(#shard{node=Node}, _) ->
                                Node =/= NodeRef
                       end, Counters),
    NewWorkerLen = WorkerLen - (fabric_dict:size(Counters) - fabric_dict:size(NewCounters)),
    case fabric_view:is_progress_possible(NewCounters) of
    true ->
        {ok, {NewWorkerLen, NewCounters, Acc}};
    false ->
        {error, {nodedown, <<"progress not possible">>}}
    end;

handle_message({ok, Count}, Shard, {WorkerLen, Counters, Acc}) ->
    case fabric_dict:lookup_element(Shard, Counters) of
    undefined ->
        % already heard from someone else in this range
        {ok, {Counters, Acc}};
    nil ->
        C1 = fabric_dict:store(Shard, ok, Counters),
        C2 = fabric_view:remove_overlapping_shards(Shard, C1),
        NewWorkerLen = WorkerLen - (fabric_dict:size(C1) - fabric_dict:size(C2)),
        case fabric_dict:any(nil, C2) andalso (NewWorkerLen > 0) of
        true ->
            {ok, {NewWorkerLen, C2, Count+Acc}};
        false ->
            {stop, Count+Acc}
        end
    end;
handle_message(_, _, Acc) ->
    {ok, Acc}.

