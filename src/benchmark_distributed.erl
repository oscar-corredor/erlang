-module(benchmark_distributed).

-export([test_get_channel_history/0,
        test_login_centralized/0,
        test_login_distributed/0,
        login_distributed/1,
        test_send_message_distributed/0,
        second_test_send_message_distributed/0,
        test_get_channel_history_distributed/0,
        initialize_server_and_some_clients_distributed/4]).



%% Benchmark helpers

% Recommendation: run each test at least 30 times to get statistically relevant
% results.
run_benchmark(Name, Fun, Times) ->
    ThisPid = self(),
    lists:foreach(fun (N) ->
        % Recommendation: to make the test fair, each new test run is to run in
        % its own, newly created Erlang process. Otherwise, if all tests run in
        % the same process, the later tests start out with larger heap sizes and
        % therefore probably do fewer garbage collections. Also consider
        % restarting the Erlang emulator between each test.
        % Source: http://erlang.org/doc/efficiency_guide/profiling.html
        spawn_link(fun () ->
            run_benchmark_once(Name, Fun, N),
            ThisPid ! done
        end),
        receive done ->
            ok
        end
    end, lists:seq(1, Times)).

run_benchmark_once(Name, Fun, N) ->
    io:format("Running benchmark ~s: ~p~n", [Name, N]),

    % Start timers
    % Tips:
    % * Wall clock time measures the actual time spent on the benchmark.
    %   I/O, swapping, and other activities in the operating system kernel are
    %   included in the measurements. This can lead to larger variations.
    %   os:timestamp() is more precise (microseconds) than
    %   statistics(wall_clock) (milliseconds)
    % * CPU time measures the actual time spent on this program, summed for all
    %   threads. Time spent in the operating system kernel (such as swapping and
    %   I/O) is not included. This leads to smaller variations but is
    %   misleading.
    statistics(runtime),        % CPU time, summed for all threads
    StartTime = os:timestamp(), % Wall clock time

    % Run
    Fun(),

    % Get and print statistics
    % Recommendation [1]:
    % The granularity of both measurement types can be high. Therefore, ensure
    % that each individual measurement lasts for at least several seconds.
    % [1] http://erlang.org/doc/efficiency_guide/profiling.html
    {_, Time1} = statistics(runtime),
    Time2 = timer:now_diff(os:timestamp(), StartTime),
    io:format("CPU time = ~p ms~nWall clock time = ~p ms~n",
        [Time1, Time2 / 1000.0]),
    io:format("~s done~n", [Name]).

% Creates a server with 10 channels and 5000 users.
initialize_server_distributed(NumberOfChannels,NumberOfUsers) ->
    rand:seed_s(exsplus, {0, 0, 0}),
    % NumberOfChannels = 10,
    % NumberOfUsers = 5000,
    ChannelNames = lists:seq(1, NumberOfChannels),
    UserNames = lists:seq(1, NumberOfUsers),
    Channels = dict:from_list(lists:map(fun (Name) ->
        Messages = [{message, 5, Name, "Hello!", os:system_time()},
                    {message, 6, Name, "Hi!", os:system_time()},
                    {message, 5, Name, "Bye!", os:system_time()}],
        Channel = {channel, Name, Messages},
        {Name, Channel}
        end,
        ChannelNames)),
    Users = dict:from_list(lists:map(fun (Name) ->
        Subscriptions = [rand:uniform(NumberOfChannels),
                         rand:uniform(NumberOfChannels),
                         rand:uniform(NumberOfChannels)],
        User = {user, Name, sets:from_list(Subscriptions)},
        {Name, User}
        end,
        UserNames)),
    % ServerPid = server_centralized:initialize_with(Users, dict:new(), Channels),
    ServerPid = main_server_distributed:initialize_with(Users, dict:new(), Channels),
    {ServerPid, Channels, Users}.

initialize_server() ->
    rand:seed_s(exsplus, {0, 0, 0}),
    NumberOfChannels = 10,
    NumberOfUsers = 5000,
    ChannelNames = lists:seq(1, NumberOfChannels),
    UserNames = lists:seq(1, NumberOfUsers),
    Channels = dict:from_list(lists:map(fun (Name) ->
        Messages = [{message, 5, Name, "Hello!", os:system_time()},
                    {message, 6, Name, "Hi!", os:system_time()},
                    {message, 5, Name, "Bye!", os:system_time()}],
        Channel = {channel, Name, Messages},
        {Name, Channel}
        end,
        ChannelNames)),
    Users = dict:from_list(lists:map(fun (Name) ->
        Subscriptions = [rand:uniform(NumberOfChannels),
                         rand:uniform(NumberOfChannels),
                         rand:uniform(NumberOfChannels)],
        User = {user, Name, sets:from_list(Subscriptions)},
        {Name, User}
        end,
        UserNames)),
    ServerPid = server_centralized:initialize_with(Users, dict:new(), Channels),
    {ServerPid, Channels, Users}.

% Creates a server with 10 channels and 5000 users, and logs in 100 users.
% `Fun(I)` is executed on the clients after log in, where I is the client's
% index, which is also its corresponding user's name.
initialize_server_and_some_clients(Fun) ->
    {ServerPid, Channels, Users} = initialize_server(),
    NumberOfActiveUsers = 100,
    BenchmarkerPid = self(),
    Clients = lists:map(fun (I) ->
        ClientPid = spawn_link(fun () ->
            server:log_in(ServerPid, I),
            BenchmarkerPid ! {logged_in, I},
            Fun(I)
        end),
        {I, ClientPid}
        end,
        lists:seq(1, NumberOfActiveUsers)),
    % Ensure that all log-ins have finished before proceeding
    lists:foreach(fun (I) ->
            receive {logged_in, I} ->
                ok
            end
        end,
        lists:seq(1, NumberOfActiveUsers)),
    {ServerPid, Channels, Users, Clients}.

% Creates a server with a certain number of channels and users, and logs in every user.
% `Fun(I)` is executed on the clients after log in, where I is the client's
% index, which is also its corresponding user's name.
initialize_server_and_some_clients_distributed(NumberOfChannels, NumberOfUsers, NumberOfActiveUsers, Fun) ->
    {ServerPid, Channels, Users} = initialize_server_distributed(NumberOfChannels, NumberOfUsers),    
    BenchmarkerPid = self(),    
    Clients = lists:map(fun (I) ->
        ClientPid = spawn_link(fun () ->
            {ChatServerPid, logged_in} = server:log_in(ServerPid, I),
            BenchmarkerPid ! {self(), logged_in, I, ChatServerPid},
            Fun(I,ChatServerPid)
        end),        
        {I, ClientPid}
        end,
        lists:seq(1, NumberOfActiveUsers)),
    %ClientsDict = initialize_clients_dict(Clients, dict:new()),
    % Ensure that all log-ins have finished before proceeding
    FinalClients = lists:map(fun (I) ->
            receive {ClientPid, logged_in, I, ChatServerPid} ->                
                {I, {ClientPid,ChatServerPid}}
            end
        end,
        lists:seq(1, NumberOfActiveUsers)),
    {ServerPid, Channels, Users, dict:from_list(FinalClients)}.

% Send a message for 1000 users, and wait for all of them to be broadcast
% (repeated 30 times).
test_send_message() ->
    run_benchmark("send_message_for_each_user",
        fun () ->
            BenchmarkerPid = self(),
            ClientFun = fun(I) ->
                receive_new_messages(BenchmarkerPid, I)
            end,
            {ServerPid, _Channels, Users, Clients} =
                initialize_server_and_some_clients(ClientFun),
            ChosenUserNames = lists:sublist(dict:fetch_keys(Users), 1000),
            send_message_for_users(ServerPid, ChosenUserNames, Users, Clients)
        end,
        30).

send_message_for_users(ServerPid, ChosenUserNames, Users, Clients) ->
    % For each of the chosen users, send a message to channel 1.
    lists:foreach(fun (UserName) ->
            server:send_message(ServerPid, UserName, 1, "Test")
        end,
        ChosenUserNames),
    % For each of the active clients, that subscribe to channel 1, wait until
    % messages arrive.
    ClientUserNames = lists:map(fun ({ClName, _ClPid}) -> ClName end, Clients),
    ClientsSubscribedTo1 = lists:filter(fun (UN) ->
        {user, UN, Subscriptions} = dict:fetch(UN, Users),
        sets:is_element(1, Subscriptions)
    end, ClientUserNames),
    lists:foreach(fun (Sender) ->
        % We expect responses from everyone except the sender.
        ExpectedResponses = lists:delete(Sender, ClientsSubscribedTo1),
        lists:foreach(fun (ClientUserName) ->
                receive {ok, ClientUserName} -> ok end
            end,
            ExpectedResponses)
        end,
        ChosenUserNames).

% Helper function: receives new messages and notifies benchmarker for each
% received message.
receive_new_messages(BenchmarkerPid, I) ->
    receive {_, new_message, _} ->
        BenchmarkerPid ! {ok, I}
    end,
    receive_new_messages(BenchmarkerPid, I).

% SEND MESSAGES DISTRIBUTED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Send a message for 1000 users, and wait for all of them to be broadcast
% (repeated 30 times).
test_send_message_distributed() ->
    ChannelsNumber =1,
    UserNumber = 1000,    
    run_benchmark("send_message_for_each_user_distributed",
        fun () ->
            BenchmarkerPid = self(),
            ClientFun = fun(I,ChatId) ->
                receive_new_messages(BenchmarkerPid, I)
            end,
            io:format("Initializing scenario~n"),
            {ServerPid, _Channels, Users, Clients} =
                initialize_server_and_some_clients_distributed(ChannelsNumber,UserNumber, UserNumber,ClientFun),
            ChosenUserNames = lists:sublist(dict:fetch_keys(Users), UserNumber),
            send_message_for_users_distributed(ServerPid, ChosenUserNames, Users, Clients)
        end,
        30).

send_message_for_users_distributed(ServerPid, ChosenUserNames, Users, Clients) ->
    % For each of the chosen users, send a message to channel 1.
    io:format("sending messages~n"), 
    lists:foreach(fun (UserName) ->
            %fetch the clients entry so we have the chat server pid
            % {UN, {ClientPid, ChatServerPid}} DICT entry with key and value            
            {ClientPid, ChatServerPid} = dict:fetch(UserName,Clients),
            %send the message            
            server:send_message(ChatServerPid, UserName, 1, "Test")
        end,
        ChosenUserNames),
    io:format("awaiting responses~n"), 
    % For each of the active clients, that subscribe to channel 1, wait until
    % messages arrive.
    % ClientUserNames = lists:map(fun ({ClName, _ClPid}) -> ClName end, Clients),
    ClientUserNames = dict:fetch_keys(Clients),
    ClientsSubscribedTo1 = lists:filter(fun (UN) ->
        {user, UN, Subscriptions} = dict:fetch(UN, Users),
        sets:is_element(1, Subscriptions)
    end, ClientUserNames),
    lists:foreach(fun (Sender) ->
        % We expect responses from everyone except the sender.
        ExpectedResponses = lists:delete(Sender, ClientsSubscribedTo1),
        lists:foreach(fun (ClientUserName) ->
                receive {ok, ClientUserName} -> ok end
            end,
            ExpectedResponses)
        end,
        ChosenUserNames).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% Get the history of 100000 channels (repeated 30 times).
test_get_channel_history() ->
    {ServerPid, Channels, _Users} = initialize_server(),
    NumberOfChannels = dict:size(Channels),
    run_benchmark("get_channel_history",
        fun () ->
            lists:foreach(fun (I) ->
                server:get_channel_history(ServerPid, I rem NumberOfChannels)
            end,
            lists:seq(1, 100000))
        end,
        30).

        
test_get_channel_history_distributed() ->
    ChannelsNumber =1,
    UserNumber = 10000,    
    run_benchmark("get_channel_history_distributed",
        fun () ->
            BenchmarkerPid = self(),
            ClientFun = fun(I, ChatServerID) ->
                server:get_channel_history(ChatServerID,1),
                BenchmarkerPid ! {done, I}
            end,
            io:format("Initializing scenario~n"),
            {ServerPid, _Channels, Users, Clients} =
                initialize_server_and_some_clients_distributed(ChannelsNumber,UserNumber, UserNumber,ClientFun),
            % Verify that every client has finished
            lists:foreach(fun (I) ->
            receive {done, I} ->
                ok
            end
        end,
        lists:seq(1, UserNumber))            
        end,
        30).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% LOGIN TEST
test_login_distributed() ->
    run_benchmark("login",
    fun() ->
            login_distributed(5000)
            end,
        30).

test_login_centralized() ->
    run_benchmark("login",
    fun() ->
            login_centralized(5000)
            end,
        30).
%test for login time
login_distributed(NumberOfActiveUsers) ->
    % Creates a server with 10 channels and 5000 users.
    {ServerPid, Channels, Users} = initialize_server_distributed(10, 5000),    
    % NumberOfActiveUsers = 100,
    BenchmarkerPid = self(),
    Clients = lists:map(fun (I) ->
        ClientPid = spawn_link(fun () ->
            server:log_in(ServerPid, I),            
            BenchmarkerPid ! {logged_in, I}            
        end),
        {I, ClientPid}
        end,
        lists:seq(1, NumberOfActiveUsers)),
    % Ensure that all log-ins have finished before proceeding
    lists:foreach(fun (I) ->
            receive {logged_in, I} ->
                ok
            end
        end,
        lists:seq(1, NumberOfActiveUsers)).

%test for login time
login_centralized(NumberOfActiveUsers) ->
    % Creates a server with 10 channels and 5000 users.
    {ServerPid, Channels, Users} = initialize_server(),
    % NumberOfActiveUsers = 100,
    BenchmarkerPid = self(),
    Clients = lists:map(fun (I) ->
        ClientPid = spawn_link(fun () ->
            server:log_in(ServerPid, I),            
            BenchmarkerPid ! {logged_in, I}            
        end),
        {I, ClientPid}
        end,
        lists:seq(1, NumberOfActiveUsers)),
    % Ensure that all log-ins have finished before proceeding
    lists:foreach(fun (I) ->
            receive {logged_in, I} ->
                ok
            end
        end,
        lists:seq(1, NumberOfActiveUsers)).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 2ND ATTEMPT AT SEND MESSAGE
second_test_send_message_distributed() ->
    ChannelsNumber =1,
    UserNumber = 18,
    MessageNumber = 1,
    run_benchmark("send_message_for_each_user_distributed",
        fun () ->
            BenchmarkerPid = self(),
            ClientFun = fun(I, ChatServerPid) ->                
                if I =< MessageNumber ->                                                
                        server:send_message(ChatServerPid, I, 1, "Test"),
                        io:format("Message sent~n"),
                        %expect 1 message less (the one it just sent)
                        receive_messages(BenchmarkerPid, MessageNumber-1,I);
                    true ->
                        %expect every message because it hasn't sent any
                        receive_messages(BenchmarkerPid, MessageNumber,I)
                end
            end,
            io:format("Initializing scenario~n"),
            {ServerPid, _Channels, Users, Clients} =
                initialize_server_and_some_clients_distributed(ChannelsNumber,UserNumber, UserNumber,ClientFun),
            %await for the ok message
            io:format("waiting for replies~n"),
            lists:foreach(fun (I) ->
                                    receive {done, I} ->
                                        io:format("received done message: ~p ~n",[I])
                                        
                                    end
                        end,
                        lists:seq(1, UserNumber))
        end,
        1).

receive_messages(BenchmarkerPid, MessageNumber,I) ->    
    if 
        MessageNumber > 0 ->            
            receive {_, new_message, _} ->                   
                receive_messages(BenchmarkerPid, MessageNumber-1,I)
            end;
        true -> 
            BenchmarkerPid ! {done, I}
    end.
        

    
    
