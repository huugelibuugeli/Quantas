// Includes function definitions for TorusPeerState class
// Manages different states of a TorusPeer

#include"TorusPeer.hpp"

namespace quantas {


// default implementations for state functions. Should never be called
void TorusPeerState::createUpChannel(json msg) {
    std::cerr << "Default createUpChannel called. This should not happen.\n";
}
void TorusPeerState::createDownChannel(json msg) {
    std::cerr << "Default createDownChannel called. This should not happen.\n";
}
void TorusPeerState::createRightChannel(json msg) {
    std::cerr << "Default createRightChannel called. This should not happen.\n";
}
void TorusPeerState::createLeftChannel(json msg) {
    std::cerr << "Default createLeftChannel called. This should not happen.\n";
}
void TorusPeerState::computation(json msg) {
    std::cerr << "Default computation called. This should not happen.\n";
}

void TorusPeerState::createChannels(json msg) {
    if (msg["location"] == "up") {
        createUpChannel(msg);
    }
    else if (msg["location"] == "down") {
        createDownChannel(msg);
    }
    else if (msg["location"] == "right") {
        createRightChannel(msg);
    }
    else if (msg["location"] == "left") {
        createLeftChannel(msg);
    }
}


//
// not joined state channel creation functions.
//
void NotJoinedState::createUpChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_downId = msg["from"];
    _peer->_downIdIndex = srcIndex;

    if (msg["myUp"] != -1) {
        _peer->_upId = msg["myUp"];
        _peer->_upIdIndex.first = msg["myUpIndex"][0];
        _peer->_upIdIndex.second = msg["myUpIndex"][1];
        _peer->_index = _peer->createIndex("up", srcIndex, _peer->_upIdIndex);
        json newChannelMsg = _peer->buildChannelPayload("up");
        std::cerr << "channel load to " << _peer->_upId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_upId);
    }
    else {
        _peer->_upId = msg["from"];
        _peer->_downId = msg["from"];
        _peer->_upIdIndex = srcIndex;
        _peer->_downIdIndex = srcIndex;
        _peer->_index = _peer->createIndex("up", srcIndex, std::make_pair(-1,-1));
    }

    _peer->rowRC(msg);
    
    std::cerr << "NOW JOINED\n";
    _peer->changeState(new JoinedState(_peer));
}
void NotJoinedState::createDownChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_upId = msg["from"];
    _peer->_upIdIndex = srcIndex;

    if (msg["myDown"] != -1) {
        _peer->_downId = msg["myDown"];
        _peer->_downIdIndex.first = msg["myDownIndex"][0];
        _peer->_downIdIndex.second = msg["myDownIndex"][1];
        _peer->_index = _peer->createIndex("down", srcIndex, _peer->_downIdIndex);
        json newChannelMsg = _peer->buildChannelPayload("down");
        std::cerr << "channel load to " << _peer->_downId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_downId);
    }
    else {
        _peer->_downId = msg["from"];
        _peer->_upId = msg["from"];
        _peer->_downIdIndex = srcIndex;
        _peer->_upIdIndex = srcIndex;
        _peer->_index = _peer->createIndex("down", srcIndex, std::make_pair(-1,-1));
    }

    _peer->rowRC(msg);

    std::cerr << "NOW JOINED\n";
    _peer->changeState(new JoinedState(_peer));
}
void NotJoinedState::createRightChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_leftId = msg["from"];
    _peer->_leftIdIndex = srcIndex;

    if (msg["myRight"] != -1) {
        _peer->_rightId = msg["myRight"];
        _peer->_rightIdIndex.first = msg["myRightIndex"][0];
        _peer->_rightIdIndex.second = msg["myRightIndex"][1];
        _peer->_index = _peer->createIndex("right", srcIndex, _peer->_rightIdIndex);
        json newChannelMsg = _peer->buildChannelPayload("right");
        std::cerr << "channel load to " << _peer->_rightId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_rightId);
    }
    else {
        _peer->_leftId = msg["from"];
        _peer->_rightId = msg["from"];
        _peer->_leftIdIndex = srcIndex;
        _peer->_rightIdIndex = srcIndex;
        _peer->_index = _peer->createIndex("right", srcIndex, std::make_pair(-1,-1));
    }

    _peer->columnRC(msg);

    std::cerr << "NOW JOINED\n";
    _peer->changeState(new JoinedState(_peer));
}
void NotJoinedState::createLeftChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_rightId = msg["from"];
    _peer->_rightIdIndex = srcIndex;

    if (msg["myLeft"] != -1) {
        _peer->_leftId = msg["myLeft"];
        _peer->_leftIdIndex.first = msg["myLeftIndex"][0];
        _peer->_leftIdIndex.second = msg["myLeftIndex"][1];
        _peer->_index = _peer->createIndex("left", srcIndex, _peer->_leftIdIndex);
        json newChannelMsg = _peer->buildChannelPayload("left");
        std::cerr << "channel load to " << _peer->_leftId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_leftId);
    }
    else {
        _peer->_leftId = msg["from"];
        _peer->_rightId = msg["from"];
        _peer->_leftIdIndex = srcIndex;
        _peer->_rightIdIndex = srcIndex;
        _peer->_index = _peer->createIndex("left", srcIndex, std::make_pair(-1,-1));
    }

    _peer->columnRC(msg);

    std::cerr << "NOW JOINED\n";
    _peer->changeState(new JoinedState(_peer));
}

// not joined state computation function
void NotJoinedState::computation(json msg) {

    if (msg["type"] == "route") {
        std::cerr << _peer->publicId() << " received route with next: " << msg["nextPeer"] << "\n";
        if (_peer->_dest.first == -1)  {
            if ((msg["funds"] > _peer->_funds && _peer->_lastMessage["funds"] < _peer->_funds) ||
                (msg["funds"] < _peer->_funds && _peer->_lastMessage["funds"] > _peer->_funds)) {
                // sets dest to node that peer wants to join
                _peer->_dest = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
                json newJoinMsg = _peer->buildJoinPayload(_peer->_dest);
                std::cerr << _peer->publicId() << " is sending join message to " << msg["from"] << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
                _peer->unicastTo(newJoinMsg, msg["from"]);
            }
            else {
                json message = _peer->buildJoinPayload({-1,-1});
                std::cerr << _peer->publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << -1 << " " << -1 << "\n";
                _peer->unicastTo(message, msg["nextPeer"]);
            }
        }
        else {
            json message = _peer->buildJoinPayload(_peer->_dest);
            std::cerr << _peer->publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
            _peer->unicastTo(message, msg["nextPeer"]);
        }
    }
    else if (msg["type"] == "channel")
        createChannels(msg);

}

void NotJoinedState::findDestination() {
    Packet packet;
    if (_peer->_readyToJoin) {
        // returns index of peer with hole that 
        // has closest funds to caller
        std::cerr << "peer: " << _peer->publicId() << " is looking for hole with closest funds\n";
        std::pair<double,double> newDest = _peer->findBestHole();
        if (newDest != _peer->_dest)
            std::cerr << "peer: " << _peer->publicId() << " found hole with index: " << newDest.first << " " << newDest.second << "\n";

        // if hole status changed to no hole, scrap all messages unless its a create channel message
        if (newDest.first == -1 && _peer->_dest.first != -1) {
            while(!_peer->inStreamEmpty()) {
                packet = _peer->popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }
            else {
            _peer->_dest = {-1,-1};
            json message = _peer->buildJoinPayload(_peer->_dest);
            std::cerr << _peer->publicId() << " is sending bootstrap join message to " << _peer->_bootStrap << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
            _peer->unicastTo(message, _peer->_bootStrap);
            }
        }
        // if hole status changed, scrap all messages unless its a create channel message
        else if (_peer->_dest.first == -1 && newDest.first != -1) {
            while (!_peer->inStreamEmpty()) {
                packet = _peer->popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }
            else {
                _peer->_dest = newDest;
                json message = _peer->buildJoinPayload(_peer->_dest);
                std::cerr << _peer->publicId() << " is sending bootstrap join message to " << _peer->_bootStrap << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
                _peer->unicastTo(message, _peer->_bootStrap);
            }
        }
    }
}

//
// joined state
//
void JoinedState::createUpChannel(json msg) {
    _peer->_upId = msg["from"];
    _peer->_upIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    if (_peer->_downId == -1) {
        _peer->_downId = _peer->_upId;
        _peer->_downIdIndex = _peer->_upIdIndex;
    }
}
void JoinedState::createDownChannel(json msg) {
    _peer->_downId = msg["from"];
    _peer->_downIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    if (_peer->_upId == -1) {
        _peer->_upId = _peer->_downId;
        _peer->_upIdIndex = _peer->_downIdIndex;
    }
}
void JoinedState::createRightChannel(json msg) {
    _peer->_rightId = msg["from"];
    _peer->_rightIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    if (_peer->_leftId == -1) {
        _peer->_leftId = _peer->_rightId;
        _peer->_leftIdIndex = _peer->_rightIdIndex;
    }
}
void JoinedState::createLeftChannel(json msg) {
    _peer->_leftId = msg["from"];
    _peer->_leftIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    if  (_peer->_rightId == -1) {
        _peer->_rightId = _peer->_leftId;
        _peer->_rightIdIndex = _peer->_leftIdIndex;
    }
}

void JoinedState::computation(json msg) {

    if (msg["type"] == "join") {
        if (msg["destination"][0] != -1) {

            // row first
            if (0 == randMod(2)) {
                if (_peer->_index.first < msg["destination"][0] && _peer->_rightId != -1 && _peer->_rightIdIndex.first > _peer->_index.first) {
                    json message = _peer->buildRoutePayload(_peer->_rightId);
                    _peer->unicastTo(message, msg["from"]);
                }
                else if (_peer->_index.first > msg["destination"][0] && _peer->_leftId != -1 && _peer->_leftIdIndex.first < _peer->_index.first) {
                    json message = _peer->buildRoutePayload(_peer->_leftId);
                    _peer->unicastTo(message, msg["from"]);
                }                    
                else if (_peer->_index.second < msg["destination"][1] && _peer->_upId != -1 && _peer->_upIdIndex.second > _peer->_index.second) {
                    json message = _peer->buildRoutePayload(_peer->_upId);
                    _peer->unicastTo(message, msg["from"]);
                }
                else if (_peer->_index.second > msg["destination"][1] && _peer->_downId != -1 && _peer->_downIdIndex.second < _peer->_index.second) {
                    json message = _peer->buildRoutePayload(_peer->_downId);
                    _peer->unicastTo(message, msg["from"]);
                }
            } 
            else {
                if (_peer->_index.second < msg["destination"][1] && _peer->_upId != -1 && _peer->_upIdIndex.second > _peer->_index.second) {
                    json message = _peer->buildRoutePayload(_peer->_upId);
                    _peer->unicastTo(message, msg["from"]);
                }
                else if (_peer->_index.second > msg["destination"][1] && _peer->_downId != -1 && _peer->_downIdIndex.second < _peer->_index.second) {
                    json message = _peer->buildRoutePayload(_peer->_downId);
                    _peer->unicastTo(message, msg["from"]);
                }
                else if (_peer->_index.first > msg["destination"][0] && _peer->_leftId != -1 && _peer->_leftIdIndex.first < _peer->_index.first) {
                    json message = _peer->buildRoutePayload(_peer->_leftId);
                    _peer->unicastTo(message, msg["from"]);
                }
                else if (_peer->_index.first < msg["destination"][0] && _peer->_rightId != -1 && _peer->_rightIdIndex.first > _peer->_index.first) {
                    json message = _peer->buildRoutePayload(_peer->_rightId);
                    _peer->unicastTo(message, msg["from"]);
                }
            } 
            if (_peer->_index.first == msg["destination"][0] && _peer->_index.second == msg["destination"][1]) {
                if (msg["funds"] > _peer->_funds) {
                    if ((_peer->_rightId == -1 && _peer->_upId == -1 )|| (_peer->_rightId != -1 && _peer->_upId != -1)) { 

                        if (randMod(2) == 0) {
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
                            json message = _peer->buildChannelPayload("right");
                            _peer->unicastTo(message, msg["from"]);
                            _peer->_rightId = msg["from"];
                            _peer->_rightIdIndex = _peer->createIndex("right", _peer->_index, _peer->_rightIdIndex);
                            if (_peer->_leftId == -1) {
                                _peer->_leftId = _peer->_rightId;
                                _peer->_leftIdIndex = _peer->_rightIdIndex;
                            }   
                        }
                        else {
                            json message = _peer->buildChannelPayload("up");
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
                            _peer->unicastTo(message, msg["from"]);
                            _peer->_upId = msg["from"];
                            _peer->_upIdIndex = _peer->createIndex("up", _peer->_index, _peer->_upIdIndex);
                            if (_peer->_downId == -1) {
                                _peer->_downId = _peer->_upId;
                                _peer->_downIdIndex = _peer->_upIdIndex;
                            }
                        }
                    }
                    else if (_peer->_rightId == -1) {
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        json message = _peer->buildChannelPayload("right");
                        _peer->unicastTo(message,msg["from"]);
                        _peer->_rightId = msg["from"];
                        _peer->_rightIdIndex = _peer->createIndex("right", _peer->_index, _peer->_rightIdIndex);
                        if (_peer->_leftId == -1) {
                            _peer->_leftId = _peer->_rightId;
                            _peer->_leftIdIndex = _peer->_rightIdIndex;
                        }
                    }
                    else if (_peer->_upId == -1) {
                        json message = _peer->buildChannelPayload("up");
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";    
                        _peer->unicastTo(message,msg["from"]);
                        _peer->_upId = msg["from"];
                        _peer->_upIdIndex = _peer->createIndex("up", _peer->_index, _peer->_upIdIndex);
                        if (_peer->_downId == -1) {
                            _peer->_downId = _peer->_upId;
                            _peer->_downIdIndex = _peer->_upIdIndex;
                        }
                    }   
                }
                else {
                    if ((_peer->_leftId == -1 && _peer->_downId == -1 )|| (_peer->_leftId != -1 && _peer->_downId != -1)) { 

                        if (randMod(2) == 0) {
                            json message = _peer->buildChannelPayload("left");
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
                            _peer->unicastTo(message, msg["from"]);
                            _peer->_leftId = msg["from"];
                            _peer->_leftIdIndex = _peer->createIndex("left", _peer->_index, _peer->_leftIdIndex);
                            if (_peer->_rightId == -1) {
                                _peer->_rightId = _peer->_leftId;
                                _peer->_rightIdIndex = _peer->_leftIdIndex;
                            }
                        }
                        else {
                            json message = _peer->buildChannelPayload("down");
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
                            _peer->unicastTo(message, msg["from"]);
                            _peer->_downId = msg["from"];
                            _peer->_downIdIndex = _peer->createIndex("down", _peer->_index, _peer->_downIdIndex);
                            if (_peer->_upId == -1) {
                                _peer->_upId = _peer->_downId;
                                _peer->_upIdIndex = _peer->_downIdIndex;
                            }
                        }
                    }
                    else if (_peer->_leftId == -1) {
                        json message = _peer->buildChannelPayload("left");
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        _peer->unicastTo(message,msg["from"]);
                        _peer->_leftId = msg["from"];
                        _peer->_leftIdIndex = _peer->createIndex("left", _peer->_index, _peer->_leftIdIndex);
                        if (_peer->_rightId == -1) {
                            _peer->_rightId = _peer->_leftId;
                            _peer->_rightIdIndex = _peer->_leftIdIndex;
                        }

                    }
                    else if (_peer->_downId == -1) {
                        json message = _peer->buildChannelPayload("down");
                        std::cerr << "building channel payload for router " << msg["from"]  << "\n";
                        _peer->unicastTo(message,msg["from"]);
                        _peer->_downId = msg["from"];
                        _peer->_downIdIndex = _peer->createIndex("down", _peer->_index, _peer->_downIdIndex);
                        if (_peer->_upId == -1) {
                            _peer->_upId = _peer->_downId;
                            _peer->_upIdIndex = _peer->_downIdIndex;
                        }
                    }   
                }
            }
        }
        else if (msg["destination"][0] == -1) {
            if (_peer->_funds >= msg["funds"]) {
                if (randMod(2) == 1) {
                    json newRouteMsg = _peer->buildRoutePayload(_peer->_rightId);
                    _peer->unicastTo(newRouteMsg,msg["from"]);
                }
                else {
                    json newRouteMsg = _peer->buildRoutePayload(_peer->_upId);
                    _peer->unicastTo(newRouteMsg,msg["from"]);
                }
            }
            else {
                if (randMod(2) == 1) {
                    json newRouteMsg = _peer->buildRoutePayload(_peer->_leftId);
                    _peer->unicastTo(newRouteMsg,msg["from"]);
                }
                else {
                    json newRouteMsg = _peer->buildRoutePayload(_peer->_downId);
                    _peer->unicastTo(newRouteMsg,msg["from"]);
                }
            }
        }
    } 
    else if (msg["type"] == "channel") {
        std::cerr << "joined peer " <<  _peer->publicId() << " received channel message from peer " << msg["from"] << " to create channel in direction " << msg["location"] << std::endl;
        createChannels(msg);
        json reply = _peer->buildResponsePayload();
        _peer->unicastTo(reply, msg["from"]);
    }
    else if (msg["type"] == "response") {

        std::cerr << _peer->publicId() << " xxx received response message from peer " << msg["from"] << " with index: " << msg["myIndex"][0] << " " << msg["myIndex"][1] << std::endl;

        if (msg["from"] == _peer->_rightId) {
            _peer->_rightIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _peer->_upId) {
            _peer->_upIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _peer->_leftId) {
            _peer->_leftIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _peer->_downId) {
            _peer->_downIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
    }
}
}
