% Copyright 2015 Cloudant
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

-module(fabric_quality_factor).

-export([go/1]).
-export([shard_sync_quality_factor/1]).

-include_lib("mem3/include/mem3.hrl").
-include_lib("couch/include/couch_db.hrl"). % user_ctx record definition
-define(ADMIN_CTX, {user_ctx, #user_ctx{roles = [<<"_admin">>]}}).

%% This module implements the calculation of a measure of replication/sync
%% quality, called the quality factor. It's an upper bound on the number of
%% missing updates in a system.
%%
%% For (simplified) example, imagine we have shard replicas A, B, and C.
%%   - Scenario 1: A, B have all updates, C is missing one. QF: 2
%%   - Scenario 2: A has all updates, B, C are missing one each. QF: 2
%%   - Scenario 3: A, B, C all up to date. QF: 0
%%
%% One can see the QF is by nature an upper bound. In scenario 1, A and B are
%% "ahead" of C by one update but we do not check if the last update to A and
%% B are identical, and so the upper bound of missing updates is 2 for the system.


%%%%%%%%%%%%
%% Module API
%%%%%%%%%%%%


%% go/1
%% This function will create rexi workers for every shard for the given
%% DbName. handle_message/3 combines per-shard quality factor calculations
%% such that fabric_util:recv should return {ok, DBQualityFactor}, where
%% DBQualityFactor is the resultant quality factor for the database.
go(DbName) ->
    Shards = mem3:shards(DbName),
    Workers = fabric_util:submit_jobs(Shards, shard_sync_quality_factor, []),
    RexiMon = fabric_util:create_monitors(Shards),
    Fun = fun handle_message/3,
    Acc0 = {fabric_dict:init(Workers, nil), []},
    try
        case fabric_util:recv(Workers, #shard.ref, Fun, Acc0) of
            {ok, DBQualityFactor} -> {ok, DBQualityFactor};
            {timeout, {WorkersDict, _}} ->
                DefunctWorkers = fabric_util:remove_done_workers(
                    WorkersDict,
                    nil
                ),
                fabric_util:log_timeout(
                    DefunctWorkers,
                    "shard_sync_quality_factor"
                ),
                {error, timeout};
            {error, Error} -> throw(Error)
        end
    after
        rexi_monitor:stop(RexiMon)
    end.


%%%%%%%%%%%%%%%%%%%%%
%% rexi callbacks
%%%%%%%%%%%%%%%%%%%%%


%% TODO is this correct when we need a response from all healthy shards?
handle_message({rexi_DOWN, _, {_,NodeRef},_}, _Shard, {Counters, Acc}) ->
    case fabric_util:remove_down_workers(Counters, NodeRef) of
    {ok, NewCounters} ->
        {ok, {NewCounters, Acc}};
    error ->
        {error, {nodedown, <<"progress not possible">>}}
    end;

%% TODO is this correct when we need a response from all healthy shards?
handle_message({rexi_EXIT, Reason}, Shard, {Counters, Acc}) ->
    NewCounters = fabric_dict:erase(Shard, Counters),
    case fabric_view:is_progress_possible(NewCounters) of
    true ->
        {ok, {NewCounters, Acc}};
    false ->
        {error, Reason}
    end;

%% handle_message/3
%%
%% We need this callback/response coordinator to listen for a response from
%% all healthy shards -- return the stop tuple only when all are accounted for.
%% Inputs:
%%   - {ok, ShardQFactor}: ShardQFactor is the integer per-shard quality
%%     factor as determined for shard Shard.
%%   - Shard: The open (shard) database. #shard.ref is used as the key for
%%     the workers dict.
%%   - {Counters, Acc}: Counters is the fabric_dict tracking per-shard
%%     response state. Acc is our total quality factor so far.
handle_message({ok, ShardQFactor}, Shard, {Counters, Acc}) ->
    %% Store the per-shard quality factor in the workers fabric_dict
    %% although we won't strictly need it there for computation.
    C1 = fabric_dict:store(Shard, ShardQFactor, Counters),
    case fabric_dict:any(nil, C1) of
        true ->
            {ok, {C1, Acc+ShardQFactor}};
        false ->
            {stop, Acc+ShardQFactor}
    end;
handle_message(_, _, Acc) ->
    {ok, Acc}.


%%%%%%%%%%%%%%%%%%%%%%
%% fabric_rpc callback
%%%%%%%%%%%%%%%%%%%%%%


%% Calculate the shard sync quality factor
%% The quality factor is an upper bound on the number of missing updates
%% between shard copies for all copies of a shard.
%% See: https://cloudant.fogbugz.com/f/cases/25615/Create-quality-factor-measurement-for-sequence#BugEvent.203899
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
%% 
%% Called by fabric_rpc:shard_sync_quality_factor
shard_sync_quality_factor(ShardName) ->
    DbName = mem3:dbname(ShardName),
    Nodes = nodes_for_shard(ShardName, DbName),

    %% For each TargetNode with a ShardName copy, find the upper bound of
    %% updates to other shard replicas which are missing from TargetNode
    MissingUpdates = [shard_sync_quality_factor(ShardName, TargetNode) ||
        TargetNode <- Nodes],

    %% The quality factor is the sum of possible missing updates
    {ok, lists:sum(MissingUpdates)}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal functions for QF determination
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% Calculate the shard sync quality factor for a single node/shard copy. This
%% is the number of updates to other shard copies not present on TargetNode
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
%% TargetNode - e.g. 'dbcore@db11.julep.cloudant.net'
shard_sync_quality_factor(not_found, _TargetNode) ->
    not_found;
shard_sync_quality_factor(ShardName, TargetNode) ->
    DbName = mem3:dbname(ShardName),
    Nodes = nodes_for_shard(ShardName, DbName),

    %% Find the upper bound of updates to other shard copies which are missing
    %% from TargetNode
    MissingUpdates = [shard_missing_updates(Source, TargetNode, ShardName) ||
        Source <- Nodes, Source =/= TargetNode],

    %% The quality factor is the sum of possible missing updates
    lists:sum(MissingUpdates).

%% Fetch a single checkpoint doc given target, source, and shard name
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
%% TargetNode - e.g. 'dbcore@db14.julep.cloudant.net'
%% SourceNode - e.g. 'dbcore@db14.julep.cloudant.net'
local_checkpoint(TargetNode, SourceNode, ShardName) ->
    {ok, Db} = rpc:call(SourceNode, couch_db, open, [ShardName, [?ADMIN_CTX]]),
    UUID = couch_db:get_uuid(Db),
    {_LocalId, Doc} = mem3_rpc:load_checkpoint(TargetNode, ShardName, SourceNode, UUID),
    Doc.

%% Find upper bound of updates to ShardName on SourceNode that are missing
%% from TargetNode, which also holds a copy of ShardName
%% SourceNode - e.g. 'dbcore@db9.julep.cloudant.net'
%% TargetNode - e.g. 'dbcore@db11.julep.cloudant.net'
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
shard_missing_updates(SourceNode, TargetNode, ShardName) ->
    %% The number of missing updates is the difference in sequence numbers.
    shard_seq(SourceNode, ShardName) - shard_checkpoint_seq(SourceNode, TargetNode, ShardName).

%% Fetch the last replicated source seq from the replication checkpoint doc.
%% SourceNode - e.g. 'dbcore@db9.julep.cloudant.net'
%% TargetNode - e.g. 'dbcore@db14.julep.cloudant.net'
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
shard_checkpoint_seq(SourceNode, TargetNode, ShardName) ->
    CheckpointDoc = local_checkpoint(TargetNode, SourceNode, ShardName),
    {Body} = element(4, CheckpointDoc),
    couch_util:get_value(<<"seq">>, Body).

%% Get the seq of a shard replica from a specific node
%% Node - e.g. 'dbcore@db14.julep.cloudant.net'
%% ShardName  - e.g. <<"shards/c0000000-ffffffff/ksnavely/motohacker.1368999314">>
shard_seq(Node, ShardName) ->
    {ok, Db} = rpc:call(Node, couch_db, open, [ShardName, [?ADMIN_CTX]]),
    couch_db:get_update_seq(Db).

nodes_for_shard(Shard, DbName) ->
    lists:map(fun(S1) -> element(3, S1) end,
        lists:filter(fun(S) ->
            case element(2, S) of
                Shard -> true;
                _ -> false
            end
        end, mem3:shards(DbName))).
