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

-module(fabric_doc_open_fdi).

-export([go/4]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

-record(acc, {
    workers,
    replies,
    r
}).

go(DbName, Id, Options0) ->
    Options = [full_doc_info | Options0],
    Shards = mem3:shards(DbName, Id),
    R = case couch_util:get_value(r, Options) of
        undefined -> length(Shards) div 2 + 1;
        Value -> erlang:min(list_to_integer(Value), length(Shards))
    end,
    Workers = fabric_util:submit_jobs(Shards, open_doc, [Id, Options]),
    RexiMon = fabric_util:create_monitors(Workers),
    Acc0 = #acc{workers=Workers, replies=[], r=R},
    try fabric_util:recv(Workers, #shard.ref, fun handle_message/3, Acc0) of
    {ok, #acc{replies=Replies, r=R}} when length(Replies) >= R ->
        {ok, Replies};
    Else ->
        Else
    after
        rexi_monitor:stop(RexiMon)
    end.


handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Worker, Acc) ->
    NewWorkers = lists:keydelete(NodeRef, #shard.node, Acc#acc.workers),
    maybe_finish(Acc#acc{workers=NewWorkers});
handle_message({rexi_EXIT, _}, Worker, Acc) ->
    NewWorkers = lists:delete(Worker, Acc#acc.workers),
    maybe_finish(Acc#acc{workers=NewWorkers});
handle_message({ok, #full_doc_info{}=FDI}, Worker, Acc) ->
    NewWorkers = lists:delete(Worker, Acc#acc.workers),
    NewReplies = [{Worker#shard{ref=undefined}, FDI} | Acc#acc.replies],
    maybe_finish(Acc#acc{workers=NewWorkers, replies=NewReplies});
handle_message(Else, Worker, Acc) ->
    NewWorkers = lists:delete(Worker, Acc#acc.workers),
    NewReplies = [{Worker#shard{ref=undefined}, Else} | Acc#acc.replies],
    maybe_finish(Acc#acc{workers=NewWorkers, replies=NewReplies}).


maybe_finish(#acc{workers=[]}=Acc) ->
    {stop, Acc};
maybe_finish(#acc{replies=Replies, r=R}) when length(Replies) >= R ->
    {stop, Acc};
maybe_finish(Acc) ->
    {ok, Acc}.
