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

-module(fabric_view_map).

-export([go/7]).

-include("fabric.hrl").
-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl").

go(DbName, GroupId, View, Args, Callback, Acc0, EncShards) when is_binary(GroupId) ->
    {ok, DDoc} = fabric:open_doc(DbName, <<"_design/", GroupId/binary>>, []),
    go(DbName, DDoc, View, Args, Callback, Acc0, EncShards);

go(DbName, DDoc, View, #view_query_args{stale=Stale, extra=Extra}=Args, Callback, Acc0, EncShards) ->
    AllShards = fabric_view:get_shards(DbName, Args),
    PrefShardsSeqs =
        if (Stale == ok orelse Stale == update_after) ->
            % stale trumps as we use primary shards
            [];
           true ->
            fabric_util:process_enc_shards(EncShards, DbName)
        end,
    Shards = fabric_util:remove_non_preferred_shards(PrefShardsSeqs, AllShards),
    Workers =
    lists:map(fun({#shard{node=Node, name=ShardName} = Shard, DbSeq}) ->
        NewExtra = lists:keystore(curr_seq, 1, Extra, {curr_seq, DbSeq}),
        Ref = rexi:cast(Node, {fabric_rpc, map_view, [ShardName, DDoc, View, Args#view_query_args{extra=NewExtra}]}),
        Shard#shard{ref = Ref}
    end, Shards),

go(DbName, DDoc, View, Args, Callback, Acc0) ->
    Shards = fabric_view:get_shards(DbName, Args),
    Workers = fabric_util:submit_jobs(Shards, map_view, [DDoc, View, Args]),
    #view_query_args{limit = Limit, skip = Skip, keys = Keys} = Args,
    State = #collector{
        db_name=DbName,
        query_args = Args,
        callback = Callback,
        counters = fabric_dict:init(Workers, 0),
        skip = Skip,
        limit = Limit,
        keys = fabric_view:keydict(Keys),
        sorted = Args#view_query_args.sorted,
        user_acc = Acc0
    },
    RexiMon = fabric_util:create_monitors(Workers),
    try rexi_utils:recv(Workers, #shard.ref, fun handle_message/3,
        State, infinity, 1000 * 60 * 60) of
    {ok, NewState} ->
        {ok, NewState#collector.user_acc};
    {timeout, NewState} ->
        Callback({error, timeout}, NewState#collector.user_acc);
    {error, Resp} ->
        {ok, Resp}
    after
        rexi_monitor:stop(RexiMon),
        fabric_util:cleanup(Workers)
    end.

handle_message({rexi_DOWN, _, {_, NodeRef}, _}, _, State) ->
    fabric_view:remove_down_shards(State, NodeRef);

handle_message({rexi_EXIT, Reason}, Worker, State) ->
    #collector{callback=Callback, counters=Counters0, user_acc=Acc} = State,
    Counters = fabric_dict:erase(Worker, Counters0),
    case fabric_view:is_progress_possible(Counters) of
    true ->
        {ok, State#collector{counters = Counters}};
    false ->
        {ok, Resp} = Callback({error, fabric_util:error_info(Reason)}, Acc),
        {error, Resp}
    end;

handle_message({total_and_offset, Tot, Off, DbSeq}, {Worker, From}, State0) ->
    #collector{
        callback = Callback,
        counters = Counters0,
        total_rows = Total0,
        offset = Offset0,
        user_acc = AccIn,
        seqs_sent = SeqsSent
    } = State0,
    case fabric_dict:lookup_element(Worker, Counters0) of
    undefined ->
        % this worker lost the race with other partition copies, terminate
        gen_server:reply(From, stop),
        {ok, State0};
    _ ->
        gen_server:reply(From, ok),
        Counters1 = fabric_dict:update_counter(Worker, 1, Counters0),
        Counters2 = fabric_view:remove_overlapping_shards(Worker, Counters1),
        State1 = fabric_util:update_seqs(DbSeq, Worker, State0),
        Total = Total0 + Tot,
        Offset = Offset0 + Off,
        case fabric_dict:any(0, Counters2) of
        true ->
            {ok, State1#collector{
                counters = Counters2,
                total_rows = Total,
                offset = Offset
            }};
        false ->
            FinalOffset = erlang:min(Total, Offset+State1#collector.skip),
            case SeqsSent of
            true ->
                Msg = {total_and_offset,
                       Total, FinalOffset};
            false ->
                Msg = {total_and_offset, Total,
                       FinalOffset, fabric_util:pack(State1#collector.seqs)}
            end,
            {Go, Acc} = Callback(Msg, AccIn),
            {Go, State1#collector{
                counters = fabric_dict:decrement_all(Counters2),
                total_rows = Total,
                offset = FinalOffset,
                user_acc = Acc,
                seqs_sent = true
            }}
        end
    end;

handle_message({#view_row{}, _DbSeq}, {_, _}, #collector{limit=0} = State) ->
    #collector{callback=Callback} = State,
    {_, Acc} = Callback(complete, State#collector.user_acc),
    {stop, State#collector{user_acc=Acc}};

handle_message(#view_row{}=Row, {Worker,From}, #collector{sorted=false}=St) ->
    #collector{
        callback = Calback,
        user_acc = AccIn,
        limit = Limit,
        seqs_sent = Sent
    } = St,
    case Sent of
        true ->
            {Go, Acc} = Callback(fabric_view:transform(Row), AccIn);
        false ->
            Seqs = St2#collector.seqs,
            {Go, Acc} = Callback({fabric_view:transform_row(Row),
                    fabric_util:pack(Seqs)}, AccIn)
    end,
    rexi:stream_ack(From),
    {Go, St#collector{user_acc=Acc, limit=Limit-1, seqs_sent=true}};

handle_message({#view_row{} = Row, DbSeq}, {Worker, From}, State) ->
    #collector{
        query_args = #view_query_args{direction=Dir},
        counters = Counters0,
        rows = Rows0,
        keys = KeyDict
    } = State,
    Rows = merge_row(Dir, KeyDict, Row#view_row{worker={Worker, From}}, Rows0),
    Counters1 = fabric_dict:update_counter(Worker, 1, Counters0),
    State1 = State#collector{rows=Rows, counters=Counters1},
    State2 = fabric_util:update_seqs(DbSeq, Worker, State1),
    fabric_view:maybe_send_row(State2);

handle_message(complete, Worker, State) ->
    Counters = fabric_dict:update_counter(Worker, 1, State#collector.counters),
    fabric_view:maybe_send_row(State#collector{counters = Counters}).

merge_row(fwd, undefined, Row, Rows) ->
    lists:merge(fun(#view_row{key=KeyA, id=IdA}, #view_row{key=KeyB, id=IdB}) ->
        couch_view:less_json([KeyA, IdA], [KeyB, IdB])
    end, [Row], Rows);
merge_row(rev, undefined, Row, Rows) ->
    lists:merge(fun(#view_row{key=KeyA, id=IdA}, #view_row{key=KeyB, id=IdB}) ->
        couch_view:less_json([KeyB, IdB], [KeyA, IdA])
    end, [Row], Rows);
merge_row(_, KeyDict, Row, Rows) ->
    lists:merge(fun(#view_row{key=A, id=IdA}, #view_row{key=B, id=IdB}) ->
        if A =:= B -> IdA < IdB; true ->
            dict:fetch(A, KeyDict) < dict:fetch(B, KeyDict)
        end
    end, [Row], Rows).
