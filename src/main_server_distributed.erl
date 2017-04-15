-module(main_server_distributed).
%http://marcelog.github.io/articles/erlang_link_vs_monitor_difference.html LINKS VS MONITORS
% -include_lib("eunit/include/eunit.hrl").
% -export([initialize/0, initialize_with/3, server_actor/3, typical_session_1/1,
%          typical_session_2/1]).
-export([initialize/0, initialize_with/3,main_server_actor/4]).

%%
%% Additional API Functions
%%

% Start server.
initialize() ->
    initialize_with(dict:new(), dict:new(), dict:new()).

% Start server with an initial state.
% Useful for benchmarking.
initialize_with(Users, LoggedIn, Channels) ->
    ServerPid = spawn_link(?MODULE, main_server_actor, [Users, LoggedIn, Channels,dict:new()]),
    catch unregister(server_actor),
    register(server_actor, ServerPid),
    ServerPid.

% * Users is a dictionary of user names to tuples of the form:
%     {user, Name, Subscriptions}
%   where Subscriptions is a set of channels that the user joined.
% * ChatServers is a dictionary of user names to tuples of the form:
%   Pid,{server,Reference,LoggedInUsers} where LoggedInUsers is the ammount of 
%   users currently logged in that particular server
% * LoggedIn is a dictionary of the names of logged in users, their pid and the
%   pid of the server they logged in to {UserPiD, ChatServerPid}
% * Channels is a dictionary of channel names to tuples:
%     {channel, Name, Messages}
%   where Messages is a list of messages, of the form:
%     {message, UserName, ChannelName, MessageText, SendTime}
main_server_actor(Users, LoggedIn, Channels, ChatServers) ->
    
    receive
        {Sender, register, Username} ->
            %create the new entry
            NewUsers = dict:store(Username, {user, Username, sets:new()}, Users),
            Sender ! {self(), user_registered},
            main_server_actor(NewUsers, LoggedIn, Channels, ChatServers);
        
        {Sender, log_in, Username} ->
            % io:format("Log in message received~n"),
            % TODO CHECK IF THE USER HAS ALREADY LOGGED IN and if it exists
            % find a server for the user
            AvailableServer = get_chat_server(dict:fetch_keys(ChatServers),ChatServers),
            case AvailableServer of
                %In case of undefined, create a new server and log the user in
                undefined ->
                    % io:format("No suitable server found~n"), 
                    {Pid, Ref} = spawn_monitor(chat_server_distributed, chat_server_actor,[dict:new(),Channels,self()]),
                    NewChatServers = dict:store(Pid,{server,Ref,1},ChatServers),
                    %log in the user in the main server
                    NewLoggedIn = dict:store(Username, {Sender, Pid}, LoggedIn),
                    %log user in the new chat server, send the users subscriptions also
                    Pid ! {self(), chat_log_in, Username, Sender, get_user_subscriptions(Username,Users)},
                    %answer to the user that he's been logged in along with the new PID for communication
                    % Sender ! {self(), logged_in, Pid},
                    Sender ! {Pid, logged_in},
                    main_server_actor(Users, NewLoggedIn, Channels, NewChatServers);

                _ ->
                    % io:format("suitable server found~n"), 
                    {Pid, {server, Reference,LoggedInUsers}} = AvailableServer,
                    NewChatServers = dict:store(Pid,{server,Reference,LoggedInUsers+1},ChatServers),
                    %log in the user in the main server
                    NewLoggedIn = dict:store(Username, {Sender, Pid}, LoggedIn),
                    %log user in the chat server
                    Pid ! {self(), chat_log_in, Username, Sender, get_user_subscriptions(Username,Users)},                    
                    %answer to the user that he's been logged in along with the new PID for communication
                    Sender ! {Pid, logged_in},
                    main_server_actor(Users, NewLoggedIn, Channels, NewChatServers)


            end;
        % syncing messages received from chat_server_distributed
        {Sender, logged_out, Username} ->        
            NewLoggedIn = dict:erase(Username, LoggedIn),
            % update the server's logged in users count
            {server, Pid, Reference,LoggedInUsers} = dict:fetch(Sender, ChatServers),
            NewChatServers = dict:store(Pid,{server,Reference, LoggedInUsers-1},ChatServers),
            main_server_actor(Users, NewLoggedIn, Channels, NewChatServers);

        {_, joined_channel, Username, ChannelName} ->
            User = dict:fetch(Username, Users), % assumes the user exists
            NewUser = join_channel(User, ChannelName),
            NewUsers = dict:store(Username, NewUser, Users),            
            main_server_actor(NewUsers, LoggedIn, Channels, ChatServers);

        {Sender, sent_message, Message} ->   
            {message, SenderName, ChannelName, MessageText, SendTime} = Message,         
            % 1. Store message in its channel
            NewChannels = store_message(Message, Channels),
            % distribute the new message accross the current chat servers
            broadcast_message_to_servers(ChatServers, {self(), new_message, SenderName, ChannelName, MessageText, SendTime}, Sender),
            main_server_actor(Users, LoggedIn, NewChannels, ChatServers)        
    end.

get_user_subscriptions(Username,Users) ->
    {user, _,Subscriptions} = dict:fetch(Username,Users),
    Subscriptions.


% * ChatServers is a dictionary of user names to tuples of the form:
%   {server, Pid, Reference,LoggedInUsers}
get_chat_server([], _) ->        
    undefined;

get_chat_server([Key|T], ChatServers) ->
    Result = dict:fetch(Key,ChatServers),
    case Result of
        {server,_,LoggedInUsers} when LoggedInUsers < 250 ->
            {Key, Result};
        _ -> get_chat_server(T,ChatServers)
    end.

join_channel({user, Name, Subscriptions}, ChannelName) ->
    {user, Name, sets:add_element(ChannelName,Subscriptions)}.

store_message(Message, Channels) ->
    {message, _UserName, ChannelName, _MessageText, _SendTime} = Message,
    {channel, ChannelName, Messages} = find_or_create_channel(ChannelName, Channels),
    dict:store(ChannelName, {channel, ChannelName, Messages ++ [Message]}, Channels).

% Broadcast `Message` to the different chat servers, except for the messages originating server.
% {Sender, new_message, Username, ChannelName, MessageText, SendTime} ->
broadcast_message_to_servers(ChatServers, Message, OriginatingServer) ->
    
    % For each LoggedIn user, fetch his subscriptions and check whether those
    % contain the channel
    Servers = fun(Pid,{server,_,_}) ->        
        OriginatingServer /= Pid        
    end,
    PendingServers = dict:filter(Servers, ChatServers),
    % Send messages
    dict:map(fun(Pid, _) ->
        Pid ! Message
    end, PendingServers),
    ok.

% Find channel, or create new channel
find_or_create_channel(ChannelName, Channels) ->
    case dict:find(ChannelName, Channels) of
        {ok, Channel} -> Channel;
        error ->         {channel, ChannelName, []}
    end.