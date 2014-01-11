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

-module(fabric_view_reduce).

-export([go/6]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(DbName, GroupId, View, Args, Callback, Acc0) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, View, Args, Callback, Acc0);

go(DbName, DDoc, VName, Args, Callback, Acc) ->
    Group = couch_view_group:design_doc_to_view_group(DDoc),
    Lang = couch_view_group:get_language(Group),
    Views = couch_view_group:get_views(Group),
    {NthRed, View} = fabric_view:extract_view(nil, VName, Views, reduce),
    {VName, RedSrc} = lists:nth(NthRed, View#view.reduce_funs),
    RPCArgs = [fabric_util:doc_id_and_rev(DDoc), VName, Args],
    Shards = fabric_view:get_shards(DbName, Args),
    Repls = fabric_view:get_shard_replacements(DbName, Shards),
    StartFun = fun(Shard) ->
        hd(fabric_util:submit_jobs([Shard], fabric_rpc2, reduce_view, RPCArgs))
    end,
    Workers0 = fabric_util:submit_jobs(Shards,fabric_rpc2,reduce_view,RPCArgs),
    RexiMon = fabric_util:create_monitors(Workers0),
    try
        case fabric_util:stream_start(Workers0, #shard.ref, StartFun, Repls) of
            {ok, Workers} ->
                try
                    go(DbName, Workers, Lang, RedSrc, Args, Callback, Acc)
                after
                    fabric_util:cleanup(Workers)
                end;
            {timeout, NewState} ->
                DefunctWorkers = fabric_util:remove_done_workers(
                    NewState#stream_acc.workers,
                    waiting
                ),
                fabric_util:log_timeout(
                    DefunctWorkers,
                    "reduce_view"
                ),
                Callback({error, timeout}, Acc);
            {error, Error} ->
                Callback({error, Error}, Acc)
        end
    after
        rexi_monitor:stop(RexiMon)
    end.

go(DbName, Workers, Lang, RedSrc, Args, Callback, Acc0) ->
    #view_query_args{limit = Limit, skip = Skip} = Args,
    OsProc = case os_proc_needed(RedSrc) of
        true -> couch_query_servers:get_os_process(Lang);
        _ -> nil
    end,
    State = #collector{
        db_name = DbName,
        query_args = Args,
        callback = Callback,
        counters = fabric_dict:init(Workers, 0),
        keys = Args#view_query_args.keys,
        skip = Skip,
        limit = Limit,
        lang = Lang,
        os_proc = OsProc,
        reducer = RedSrc,
        rows = dict:new(),
        user_acc = Acc0
    },
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, NewState} ->
        {ok, NewState#collector.user_acc};
    {timeout, NewState} ->
        Callback({error, timeout}, NewState#collector.user_acc);
    {error, Resp} ->
        {ok, Resp}
    after
        if OsProc == nil -> ok; true ->
            catch couch_query_servers:ret_os_process(OsProc)
        end
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _, State) ->
    fabric_view:check_down_shards(State, NodeRef);

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    fabric_view:handle_worker_exit(State, Worker, Reason);

handle_message(#view_row{key=Key} = Row, {Worker, From}, State) ->
    #collector{counters = Counters0, rows = Rows0} = State,
    true = fabric_dict:is_key(Worker, Counters0),
    Rows = dict:append(Key, Row#view_row{worker={Worker, From}}, Rows0),
    C1 = fabric_dict:update_counter(Worker, 1, Counters0),
    State1 = State#collector{rows=Rows, counters=C1},
    fabric_view:maybe_send_row(State1);

handle_message(complete, Worker, #collector{counters = Counters0} = State) ->
    true = fabric_dict:is_key(Worker, Counters0),
    C1 = fabric_dict:update_counter(Worker, 1, Counters0),
    fabric_view:maybe_send_row(State#collector{counters = C1}).

os_proc_needed(<<"_", _/binary>>) -> false;
os_proc_needed(_) -> true.

