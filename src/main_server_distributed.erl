-module(main_server_distributed).
%http://marcelog.github.io/articles/erlang_link_vs_monitor_difference.html LINKS VS MONITORS
% -include_lib("eunit/include/eunit.hrl").
% -export([initialize/0, initialize_with/3, server_actor/3, typical_session_1/1,
%          typical_session_2/1]).
-export([main_server_actor/4]).


% * Users is a dictionary of user names to tuples of the form:
%     {user, Name, Subscriptions}
% * ChatServers is a dictionary of user names to tuples of the form:
%   {server, Pid, Reference,LoggedInUsers} where LoggedInUsers is the ammount of 
%   users currently logged in that particular server
% * LoggedIn is a dictionary of the names of logged in users, their pid and the
%   pid of the server they logged in to {UserPiD, ChatServerPid}
main_server_actor(Users, LoggedIn, Channels, ChatServers) ->
    
    receive
        {Sender, register, Username} ->
            %create the new entry
            NewUsers = dict:store(Username, {user, Username, sets:new()}, Users),
            Sender ! {self(), user_registered},
            main_server_actor(NewUsers, LoggedIn, Channels, ChatServers);
        
        {Sender, log_in, Username} ->
            io:format("Log in message received"),
            % TODO CHECK IF THE USER HAS ALREADY LOGGED IN and if it exists
            % find a server for the user
            AvailableServer = get_chat_server(dict:fetch_keys(ChatServers),ChatServers),
            case AvailableServer of
                %In case of undefined, create a new server and log the user in
                undefined ->
                    io:format("No suitable server found"), 
                    {Pid, Ref} = spawn_monitor(chat_server_distributed, chat_server_actor,[Users,dict:new(),Channels,self()]),
                    NewChatServers = dict:store(Pid,{server, Pid, Ref,1},ChatServers),
                    %log in the user in the main server
                    NewLoggedIn = dict:store(Username, {Sender, Pid}, LoggedIn),
                    %log user in the new chat server
                    Pid ! {self(), chat_log_in, Username, UserProcess},
                    %answer to the user that he's been logged in along with the new PID for communication
                    Sender ! {self(), logged_in, Pid},
                    main_server_actor(Users, NewLoggedIn, Channels, NewChatServers);

                _ ->
                    io:format("suitable server found"), 
                    {server, Pid, Reference,LoggedInUsers} = AvailableServer,
                    %for now we'll remove and insert the key again although this is extremely inefficient
                    PreServers = dict:erase(Pid,ChatServers),
                    NewChatServers = dict:store(Pid,{server,Pid,Reference,LoggedInUsers+1},PreServers),
                    %log in the user in the main server
                    NewLoggedIn = dict:store(Username, {Sender, Pid}, LoggedIn),
                    %log user in the chat server
                    Pid ! {self(), chat_log_in,Username},
                    %answer to the user that he's been logged in along with the new PID for communication
                    Sender ! {self(), logged_in, Pid},
                    main_server_actor(Users, NewLoggedIn, Channels, NewChatServers)


            end
    end.

% * ChatServers is a dictionary of user names to tuples of the form:
%   {server, Pid, Reference,LoggedInUsers}
get_chat_server([], _) ->        
    undefined;

get_chat_server([Key|T], ChatServers) ->
    Result = dict:fetch(Key,ChatServers),
    case Result of
        {server,_,_,LoggedInUsers} when LoggedInUsers < 3 ->
            Result;
        _ -> get_chat_server(T,ChatServers)
    end.