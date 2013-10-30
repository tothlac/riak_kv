%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Code related to repairing 2i data.
-module(riak_kv_2i_aae).

-include("riak_kv_wm_raw.hrl").

-export([repair_partition/2, start_partition_repair/2]).

%% How many items to scan before possibly pausing to obey speed throttle
-define(DEFAULT_SCAN_BATCH, 1000).
-define(MAX_DUTY_CYCLE, 100).
-define(MIN_DUTY_CYCLE, 1).
-define(LOCK_RETRIES, 10).

%% @doc Starts a process that will issue a repair for each partition in the
%% list, sequentially.
%% The DutyCycle parameter controls how fast we go. 100 means full speed,
%% 50 means to do a unit of work and pause for the duration of that unit
%% to achieve a 50% duty cycle, etc.
-spec start_partition_repair([integer()], integer()) -> pid().
start_partition_repair(Partitions, DutyCycle)
  when is_number(DutyCycle),
       DutyCycle >= ?MIN_DUTY_CYCLE, DutyCycle =< ?MAX_DUTY_CYCLE ->
    spawn(
      fun() ->
              %% Poor man's lock on this operation.
              true = register(riak_kv_2i_aae_repair, self()),
              try
                  lager:info("Starting 2i repair at speed ~p for partitions ~p",
                             [DutyCycle, Partitions]),
                  Errors =
                  lists:foldl(
                    fun(Partition, Errors) ->
                            case (catch repair_partition(Partition,
                                                         DutyCycle)) of
                                ok ->
                                    Errors;
                                {error, Err} ->
                                    Errors ++ [{Partition, Err}];
                                Err ->
                                    Errors ++ [{Partition, Err}]
                            end
                    end, [], Partitions),
                  ErrMsg =
                  case Errors of
                      [] -> "";
                      _ -> io_lib:format(" with errors ~p", [Errors])
                  end,
                  lager:info("Finished 2i repair for partitions ~p~s",
                             [Partitions, ErrMsg])
              catch
                  _:_ ->
                      lager:error("Could not complete 2i repair for partitions ~p",
                                  [Partitions])
              end
      end).

repair_partition(Partition, DutyCycle)
  when is_number(DutyCycle),
       DutyCycle >= ?MIN_DUTY_CYCLE, DutyCycle =< ?MAX_DUTY_CYCLE ->
    lager:info("Repairing 2i indexes in partition ~p", [Partition]),
    case create_index_data_db(Partition, DutyCycle) of
        {ok, {DBDir, DBRef}} ->
            Res =
            case build_tmp_tree(Partition, DBRef, DutyCycle) of
                {ok, TmpTree} ->
                    Res0 =
                    case riak_kv_vnode:hashtree_pid(Partition) of
                        {ok, TreePid} ->
                            run_exchange(Partition, DBRef, TmpTree, TreePid);
                        _ ->
                            {error, no_tree_for_partition}
                    end,
                    remove_tmp_tree(Partition, TmpTree),
                    Res0;
                Err ->
                    Err
            end,
            destroy_index_data_db(DBDir, DBRef),
            lager:info("Finished repairing indexes in partition ~p", [Partition]),
            Res;
        Err ->
            Err
    end.

%% @doc Run exchange between temporary 2i hashtree and a vnode's 2i hashtree.
run_exchange(Partition, DBRef, TmpTree, TreePid) ->
    lager:info("Reconciling 2i data"),
    Self = self(),
    Ref = make_ref(),
    WorkerPid =
    spawn(
      fun() ->
              WRes = do_exchange(Partition, DBRef, TmpTree, TreePid),
              Self ! {Ref, WRes}
      end),
    Mon = monitor(process, WorkerPid),
    receive
        {'DOWN', Mon, process, WorkerPid, Reason} ->
            lager:error("2i repair worker failed : ~p", [Reason]),
            {error, Reason};
        {Ref, WRes} ->
            demonitor(Mon, [flush]),
            WRes
    end.

%% No wait for speed 100, wait equal to work time when speed is 50,
%% wait equal to 99 times the work time when speed is 1.
wait_factor(DutyCycle) ->
    if
        %% Avoid potential floating point funkiness for full speed.
        DutyCycle >= ?MAX_DUTY_CYCLE -> 0;
        true -> (100 - DutyCycle) / DutyCycle
    end.

scan_batch() ->
    app_helper:get_env(riak_kv, aae_2i_batch_size, ?DEFAULT_SCAN_BATCH).

%% @doc Create temporary DB holding 2i index data from disk.
create_index_data_db(Partition, DutyCycle) ->
    DBDir = filename:join(data_root(), "tmp_db"),
    catch eleveldb:destroy(DBDir, []),
    case filelib:ensure_dir(DBDir) of
        ok ->
            create_index_data_db(Partition, DutyCycle, DBDir);
        _ ->
            {error, io_lib:format("Could not create directory ~s", [DBDir])}
    end.

create_index_data_db(Partition, DutyCycle, DBDir) ->
    lager:info("Creating temporary database of 2i data in ~s", [DBDir]),
    DBOpts = [{create_if_missing, true}, {error_if_exists, true}],
    {ok, DBRef} = eleveldb:open(DBDir, DBOpts),
    Client = self(),
    BatchRef = make_ref(),
    WaitFactor = wait_factor(DutyCycle),
    ScanBatch = scan_batch(),
    Fun =
    fun(Bucket, Key, Field, Term, Count) ->
            BKey = term_to_binary({Bucket, Key}),
            OldVal = fetch_index_data(BKey, DBRef),
            Val = [{Field, Term}|OldVal],
            ok = eleveldb:put(DBRef, BKey, term_to_binary(Val), []),
            Count2 = Count + 1,
            case Count2 rem ScanBatch of
                0 when DutyCycle < ?MAX_DUTY_CYCLE ->
                    Mon = monitor(process, Client),
                    Client ! {BatchRef, self()},
                    receive
                        {BatchRef, continue} ->
                            ok;
                        {'DOWN', Mon, _, _, _} ->
                            throw(fold_receiver_died)
                    end,
                    demonitor(Mon, [flush]);
                _ ->
                    ok
            end,
            Count2
    end,
    lager:info("Grabbing all index data for partition ~p", [Partition]),
    Ref = make_ref(),
    Sender = {raw, Ref, Client},
    StartTime = now(),
    riak_core_vnode_master:command({Partition, node()},
                                   {fold_indexes, Fun, 0},
                                   Sender,
                                   riak_kv_vnode_master),
    NumFound = wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor),
    case NumFound of
        {error, _} = Err ->
            destroy_index_data_db(DBDir, DBRef),
            Err;
        _ ->
            lager:info("Grabbed ~p index data entries from partition ~p",
                       [NumFound, Partition]),
            {ok, {DBDir, DBRef}}
    end.

duty_cycle_pause(WaitFactor, StartTime) ->
    case WaitFactor > 0 of
        true ->
            Now = now(),
            ElapsedMicros = timer:now_diff(Now, StartTime),
            WaitMicros = ElapsedMicros * WaitFactor,
            WaitMillis = trunc(WaitMicros / 1000 + 0.5),
            timer:sleep(WaitMillis);
        false ->
            ok
    end.

wait_for_index_scan(Ref, BatchRef, StartTime, WaitFactor) ->
    receive
        {BatchRef, Pid} ->
            duty_cycle_pause(WaitFactor, StartTime),
            Pid ! {BatchRef, continue},
            wait_for_index_scan(Ref, BatchRef, now(), WaitFactor);
        {Ref, Result} ->
            Result
    end.

fetch_index_data(BK, DBRef) ->
    case eleveldb:get(DBRef, BK, []) of
        {ok, BinVal} ->
            binary_to_term(BinVal);
        _ ->
            []
    end.

%% @doc Remove all traces of the temporary 2i index DB
destroy_index_data_db(DBDir, DBRef) ->
    catch eleveldb:close(DBRef),
    eleveldb:destroy(DBDir, []).

%% @doc Returns the base data directory to use inside the AAE directory.
data_root() ->
    BaseDir = riak_kv_index_hashtree:determine_data_root(),
    filename:join(BaseDir, "2i").

%% @doc Builds a temporary hashtree based on the 2i data fetched from the
%% backend.
build_tmp_tree(Index, DBRef, DutyCycle) ->
    lager:info("Building tree for 2i data on disk for partition ~p", [Index]),
    %% Build temporary hashtree with this index data.
    TreeId = <<0:176/integer>>,
    Path = filename:join(data_root(), integer_to_list(Index)),
    catch eleveldb:destroy(Path, []),
    case filelib:ensure_dir(Path) of
        ok ->
            Tree = hashtree:new({Index, TreeId}, [{segment_path, Path}]),
            {ok, Itr} = eleveldb:iterator(DBRef, []),
            WaitFactor = wait_factor(DutyCycle),
            ScanBatch = scan_batch(),
            Tree2 = tmp_tree_insert(tmp_tree_iter_move(Itr, <<>>),
                                    {Itr, 0, ScanBatch, WaitFactor, now()}, Tree),
            eleveldb:iterator_close(Itr),
            lager:info("Done building temporary tree for 2i data"),
            {ok, Tree2};
        _ ->
            {error, io_lib:format("Could not create directory ~s", [Path])}
    end.

%% @doc Wraps the leveldb iterator move operation.
tmp_tree_iter_move(Itr, Seek) ->
    try
        eleveldb:iterator_move(Itr, Seek)
    catch
        _:badarg ->
            {error, invalid_iterator}
    end.

%% @doc Iteration worker that inserts index data entries from the temporary
%% database into the temporary 2i hashtree.
tmp_tree_insert({error, invalid_iterator}, _ItrCtx, Tree) ->
    Tree;
tmp_tree_insert({ok, K, V},
                {Itr, Count, BatchSize, WaitFactor, StartTime},
                Tree) ->
    Indexes = binary_to_term(V),
    Hash = riak_kv_index_hashtree:hash_index_data(Indexes),
    Tree2 = hashtree:insert(K, Hash, Tree),
    Count2 = Count + 1,
    StartTime2 =
    case Count2 rem BatchSize of
        0 -> duty_cycle_pause(WaitFactor, StartTime),
             now();
        _ -> StartTime
    end,
    tmp_tree_insert(tmp_tree_iter_move(Itr, next),
                    {Itr, Count2, BatchSize, WaitFactor, StartTime2}, Tree2).

%% @doc Remove all traces of the temporary hashtree for 2i index data.
remove_tmp_tree(Partition, Tree) ->
    lager:debug("Removing temporary AAE 2i tree for partition ~p", [Partition]),
    hashtree:close(Tree),
    hashtree:destroy(Tree).

%% @doc Run exchange between the temporary 2i tree and the vnode's 2i tree.
do_exchange(Partition, DBRef, TmpTree, TreePid) ->
    case get_hashtree_lock(TreePid, ?LOCK_RETRIES) of
        ok ->
            IndexN2i = riak_kv_index_hashtree:index_2i_n(),
            lager:debug("Updating the vnode tree"),
            ok = riak_kv_index_hashtree:update(IndexN2i, TreePid),
            lager:debug("Updating the temporary 2i AAE tree"),
            {_, TmpTree2} = hashtree:update_snapshot(TmpTree),
            TmpTree3 = hashtree:update_perform(TmpTree2),
            Remote =
            fun(get_bucket, {L, B}) ->
                    R1 = riak_kv_index_hashtree:exchange_bucket(IndexN2i, L, B,
                                                                TreePid),
                    R1;
               (key_hashes, Segment) ->
                    R2 = riak_kv_index_hashtree:exchange_segment(IndexN2i, Segment,
                                                                 TreePid),
                    R2;
               (A, B) ->
                    throw({riak_kv_2i_aae_internal_error, A, B})
            end,
            AccFun =
            fun(KeyDiff, Acc) ->
                    lists:foldl(
                      fun({_DiffReason, BKeyBin}, Count) ->
                              BK = binary_to_term(BKeyBin),
                              IdxData = fetch_index_data(BKeyBin, DBRef),
                              riak_kv_vnode:refresh_index_data(Partition, BK, IdxData),
                              Count + 1
                      end,
                      Acc, KeyDiff)
            end,
            NumDiff = hashtree:compare(TmpTree3, Remote, AccFun, 0),
            lager:info("Found ~p differences", [NumDiff]),
            ok;
        LockErr ->
            {error, LockErr}
    end.


get_hashtree_lock(TreePid, Retries) ->
    case riak_kv_index_hashtree:get_lock(TreePid, local_fsm) of
        Reply = already_locked ->
            case Retries > 0 of
                true ->
                    timer:sleep(1000),
                    get_hashtree_lock(TreePid, Retries-1);
                false ->
                    Reply
            end;
        Reply ->
            Reply
    end.
