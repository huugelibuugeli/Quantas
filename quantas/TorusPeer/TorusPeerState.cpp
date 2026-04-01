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


void NotJoinedState::preComputation() {
            
    if (_peer->_readyToJoin) {
        if (!_createdChannel) {
            findDestination();
            //std::cerr << "_peer address: " << _peer << ", publicId: " << _peer->publicId() << "\n";
            //std::cerr << "ATTEMPTING TO ACCESS PEER MEMBER VARIABLE DESTINATION: " << _peer->_dest.first << " " << _peer->_dest.second << std::endl;
            //std::cerr << "SUCCESS\n";
            if (_peer->_destHasValue && !_peer->_startedSearch) {
                //std::cerr << "SENDING PATH FIND PAYLOAD\n";
                _peer->_startedSearch = true;
                _peer->_visited.insert(std::make_pair(_peer->_bootStrap, std::make_pair(0,0)));
                json msg = _peer->buildPathFindPayload(_peer->_dest);
                _peer->unicastTo(msg,_peer->_bootStrap);
            }
        }
        else
            _peer->changeState();
    }
}

//
// not joined state channel creation functions.
//
void NotJoinedState::createUpChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_downId = msg["from"];
    _peer->_downIdIndex = srcIndex; _peer->_downIndexHasValue = true;

    _peer->_horizontalSteps = msg["horizontalSteps"];
    _peer->_verticalSteps = msg["verticalSteps"];
    //++_peer->_verticalSteps;
    std::pair<int,int> steps = std::make_pair(_peer->_horizontalSteps, _peer->_verticalSteps);
    //std::cerr << "NOT JOINED PEER CREATING INDEX WITH STEPS " << _peer->_horizontalSteps << " " << _peer->_verticalSteps << std::endl;

    if (msg["myUp"] != -1) {

        _peer->_upId = msg["myUp"];
        _peer->_upIdIndex.first = msg["myUpIndex"][0];
        _peer->_upIdIndex.second = msg["myUpIndex"][1]; _peer->_upIndexHasValue = true;
        _peer->_index = _peer->createIndex("up", srcIndex);
        json newChannelMsg = _peer->buildChannelPayload("up");
        //std::cerr << "channel load to " << _peer->_upId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_upId);
    }
    else {
        ++_peer->_verticalSteps; ++steps.second;
        _peer->_upId = msg["from"];
        _peer->_downId = msg["from"];
        _peer->_upIdIndex = srcIndex;   _peer->_upIndexHasValue = true;
        _peer->_index = _peer->createIndex("up", srcIndex);
    }

    _peer->rowRC(msg);
    
    //std::cerr << "NOW JOINED\n";
    //_peer->changeState();
    _createdChannel = true;
}
void NotJoinedState::createDownChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_upId = msg["from"];
    _peer->_upIdIndex = srcIndex; _peer->_upIndexHasValue = true;

    _peer->_horizontalSteps = msg["horizontalSteps"];
    _peer->_verticalSteps = msg["verticalSteps"];
    //--_peer->_verticalSteps;
    std::pair<int,int> steps = std::make_pair(_peer->_horizontalSteps, _peer->_verticalSteps);
    //std::cerr << "NOT JOINED PEER CREATING INDEX WITH STEPS " << _peer->_horizontalSteps << " " << _peer->_verticalSteps << std::endl;

    if (msg["myDown"] != -1) {

        // if true essentially same as taking down step
        // see createIndex() for why
        if (msg["myDownIndex"][1] > srcIndex.second)
            --_peer->_verticalSteps;
        _peer->_downId = msg["myDown"];
        _peer->_downIdIndex.first = msg["myDownIndex"][0];
        _peer->_downIdIndex.second = msg["myDownIndex"][1]; _peer->_downIndexHasValue = true;
        _peer->_index = _peer->createIndex("down", srcIndex);
        json newChannelMsg = _peer->buildChannelPayload("down");
        //std::cerr << "channel load to " << _peer->_downId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_downId);
    }
    else {
        --_peer->_verticalSteps; --steps.second;
        _peer->_downId = msg["from"];
        _peer->_upId = msg["from"];
        _peer->_downIdIndex = srcIndex; _peer->_downIndexHasValue = true;
        _peer->_index = _peer->createIndex("down", srcIndex);
    }

    _peer->rowRC(msg);

    //std::cerr << "NOW JOINED\n";
    //_peer->changeState();
    _createdChannel = true;

}
void NotJoinedState::createRightChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_leftId = msg["from"];
    _peer->_leftIdIndex = srcIndex; _peer->_leftIndexHasValue = true;

    _peer->_horizontalSteps = msg["horizontalSteps"];
    _peer->_verticalSteps = msg["verticalSteps"];
    //++_peer->_horizontalSteps;
    std::pair<int,int> steps = std::make_pair(_peer->_horizontalSteps, _peer->_verticalSteps);
    //std::cerr << "NOT JOINED PEER CREATING INDEX WITH STEPS " << _peer->_horizontalSteps << " " << _peer->_verticalSteps << std::endl;

    if (msg["myRight"] != -1) {
        if (msg["myRightIndex"][0] < srcIndex.first)
            ++_peer->_horizontalSteps;
        _peer->_rightId = msg["myRight"];
        _peer->_rightIdIndex.first = msg["myRightIndex"][0];
        _peer->_rightIdIndex.second = msg["myRightIndex"][1]; _peer->_rightIndexHasValue = true;
        _peer->_index = _peer->createIndex("right", srcIndex);
        json newChannelMsg = _peer->buildChannelPayload("right");
        std::cerr << "channel load to " << _peer->_rightId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_rightId);
    }
    else {
        ++_peer->_horizontalSteps; ++steps.first;
        _peer->_rightId = msg["from"];
        _peer->_rightIdIndex = srcIndex; _peer->_rightIndexHasValue = true;
        _peer->_index = _peer->createIndex("right", srcIndex);
    }

    _peer->columnRC(msg);

    //std::cerr << "NOW JOINED\n";
    //_peer->changeState();
    _createdChannel = true;
}
void NotJoinedState::createLeftChannel(json msg) {
    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_rightId = msg["from"];
    _peer->_rightIdIndex = srcIndex; _peer->_rightIndexHasValue = true;

    _peer->_horizontalSteps = msg["horizontalSteps"];
    _peer->_verticalSteps = msg["verticalSteps"];
    //--_peer->_horizontalSteps;
    std::pair<int,int> steps = std::make_pair(_peer->_horizontalSteps, _peer->_verticalSteps);
    //std::cerr << "NOT JOINED PEER CREATING INDEX WITH STEPS " << _peer->_horizontalSteps << " " << _peer->_verticalSteps << std::endl;

    if (msg["myLeft"] != -1) {
        if (msg["myLeftIndex"][0] > srcIndex.first)
            --_peer->_horizontalSteps;
        _peer->_leftId = msg["myLeft"];
        _peer->_leftIdIndex.first = msg["myLeftIndex"][0];
        _peer->_leftIdIndex.second = msg["myLeftIndex"][1]; _peer->_leftIndexHasValue = true;
        _peer->_index = _peer->createIndex("left", srcIndex);
        json newChannelMsg = _peer->buildChannelPayload("left");
        //std::cerr << "channel load to " << _peer->_leftId << "\n";
        _peer->unicastTo(newChannelMsg, _peer->_leftId);
    }
    else {
        _peer->_horizontalSteps--; --steps.first; 
        _peer->_leftId = msg["from"];
        _peer->_leftIdIndex = srcIndex; _peer->_leftIndexHasValue = true;
        _peer->_index = _peer->createIndex("left", srcIndex);
    }

    _peer->columnRC(msg);

    //std::cerr << "NOW JOINED\n";
    //_peer->changeState();
    _createdChannel = true;
}

// not joined state computation function
void NotJoinedState::computation(json msg) {

    if (msg["type"] == "route") {
        std::cerr << _peer->publicId() << " received route with next: " << msg["nextPeer"] << "\n";
        //if (_peer->_dest.first == -1)  {
            if ((msg["funds"] > _peer->_funds && _peer->_lastMessage["funds"] < _peer->_funds) ||
                (msg["funds"] < _peer->_funds && _peer->_lastMessage["funds"] > _peer->_funds)) {
                // sets dest to node that peer wants to join
                _peer->_dest = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
                json newJoinMsg = _peer->buildJoinPayload(_peer->_dest);
                //std::cerr << _peer->publicId() << " is sending join message to " << msg["from"] << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
                _peer->unicastTo(newJoinMsg, msg["from"]);
            }
            else {
                json message = _peer->buildJoinPayload({-1,-1});
                //std::cerr << _peer->publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << -1 << " " << -1 << "\n";
                _peer->unicastTo(message, msg["nextPeer"]);
            }
        //}
        /*else {
            json message = _peer->buildJoinPayload(_peer->_dest);
            std::cerr << _peer->publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << _peer->_dest.first << " " << _peer->_dest.second << "\n";
            _peer->unicastTo(message, msg["nextPeer"]);
        }*/
    }
    if (msg["type"] == "pathFindResponse") {
        //std::cerr << _peer->publicId() << " received path find response from peer " << msg["from"] << " with index: " << msg["myIndex"][0] << " " << msg["myIndex"][1] << std::endl;
        _peer->pathFind(msg);
    }
    else if (msg["type"] == "channel") {
        //std::cerr << "NOT JOINED PEER RECEIVED CHANNEL MESSAGE" << std::endl;
        createChannels(msg);
    }

}

void NotJoinedState::findDestination() {
    Packet packet;
    if (_peer->_readyToJoin) {
        // returns index of peer with hole that 
        // has closest funds to caller
        //std::cerr << "peer: " << _peer->publicId() << " is looking for hole with closest funds\n";
        auto newDest = _peer->findBestHole();
        if (newDest.second && ((_peer->_destHasValue && newDest.first != _peer->_dest) || !_peer->_destHasValue)) {
            //std::cerr << "peer: " << _peer->publicId() << " found hole with index: " << newDest.first.first << " " << newDest.first.second << "\n";
            _peer->_dest = newDest.first;
            _peer->_destHasValue = true;

            //std::cerr << "MADE TJEITJETHIEHQTIOHQTH \n";

            size_t dequeSize = _peer->_toVisit.size();
            _peer->clearToVisit();

            //std::cerr << " dasdasfasf\n";

            auto it = _peer->_visited.begin();
            while (it != _peer->_visited.end()) {
                it = _peer->_visited.erase(it);
            }

            _peer->_startedSearch = false;

            while(!_peer->inStreamEmpty()) {
                packet = _peer->popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }

        }

        // if hole status changed, scrap all messages unless its a create channel message
        if (!newDest.second) {
            _peer->_destHasValue = false;
            std::cerr << "STARTING SEARCH WITHOUT DESTINATION\n";
            std::cerr << "SEARCHING WITH FUNDS: " << _peer->_funds;
            while (!_peer->inStreamEmpty() && _peer->_dest.first != -1) {
                packet = _peer->popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }
            else if (!_startedRoute) {
                _startedRoute = true;
                _peer->_dest = newDest.first;
                json message = _peer->buildJoinPayload(_peer->_dest);
                std::cerr << _peer->publicId() << " is sending bootstrap join message to " << _peer->_bootStrap << "\n";
                _peer->unicastTo(message, _peer->_bootStrap);
            }
        }
        

        std::cerr << " end of findDestination call\n";

        /*
        
        // if hole status changed to no hole, scrap all messages unless its a create channel message
        if (newDest.first == -1 && _peer->_dest.first != -1) {
            while(!_peer->inStreamEmpty()) {
                packet = _peer->popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }/*
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
        }*/
    }
}

//
// joined state
//
void JoinedState::createUpChannel(json msg) {
    _peer->_downId = msg["from"];
    _peer->_downIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_downIndexHasValue = true;
    if (_peer->_upId == -1) {
        _peer->_upId = _peer->_downId;
        _peer->_upIdIndex = _peer->_downIdIndex;
        _peer->_upIndexHasValue = true;
    }
}
void JoinedState::createDownChannel(json msg) {
    _peer->_upId = msg["from"];
    _peer->_upIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]); 
    _peer->_upIndexHasValue = true;
    if (_peer->_downId == -1) {
        _peer->_downId = _peer->_upId;
        _peer->_downIdIndex = _peer->_upIdIndex;
        _peer->_downIndexHasValue = true;
    }
}
void JoinedState::createRightChannel(json msg) {
    _peer->_leftId = msg["from"];
    _peer->_leftIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_leftIndexHasValue = true;
    if (_peer->_rightId == -1) {
        _peer->_rightId = _peer->_leftId;
        _peer->_rightIdIndex = _peer->_leftIdIndex;
        _peer->_rightIndexHasValue = true;
    }
}
void JoinedState::createLeftChannel(json msg) {
    _peer->_rightId = msg["from"];
    _peer->_rightIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
    _peer->_rightIndexHasValue = true;
    if  (_peer->_leftId == -1) {
        _peer->_leftId = _peer->_rightId;
        _peer->_leftIdIndex = _peer->_rightIdIndex;
        _peer->_leftIndexHasValue = true;
    }
}

void JoinedState::computation(json msg) {

    if (msg["type"] == "join") {
        if (_peer->_index.first == msg["destination"][0] && _peer->_index.second == msg["destination"][1]) {
            //std::cerr << "JOINED PEER MAKKING STEPS WITH " << _peer->_horizontalSteps << " " << _peer->_verticalSteps << std::endl;
            std::pair<int,int> steps = std::make_pair(_peer->_horizontalSteps, _peer->_verticalSteps);

            if (msg["funds"] > _peer->_funds) {
                //std::cerr << "LARGER JOIN\n";
                if (_peer->hasHoleLocation("right") && _peer->hasHoleLocation("up")) { 

                    if (randMod(2) == 0) {
                        //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        json message = _peer->buildChannelPayload("right");
                        _peer->unicastTo(message, msg["from"]);
                        _peer->_rightId = msg["from"];
                        steps.first++;
                        _peer->_rightIdIndex = _peer->createIndex("right", _peer->_index);
                        _peer->_rightIndexHasValue = true;
                        if (_peer->_leftId == -1) {
                            _peer->_leftId = _peer->_rightId;
                            _peer->_leftIdIndex = _peer->_rightIdIndex;
                            _peer->_leftIndexHasValue = true;
                        }   
                    }
                    else {
                        json message = _peer->buildChannelPayload("up");
                        //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        _peer->unicastTo(message, msg["from"]);
                        _peer->_upId = msg["from"];
                        steps.second++;
                        _peer->_upIdIndex = _peer->createIndex("up", _peer->_index);
                        _peer->_upIndexHasValue = true;
                        if (_peer->_downId == -1) {
                            _peer->_downId = _peer->_upId;
                            _peer->_downIdIndex = _peer->_upIdIndex;
                            _peer->_downIndexHasValue = true;
                        }
                    }
                }
                else if (_peer->hasHoleLocation("right")) {
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                    json message = _peer->buildChannelPayload("right");
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_rightId = msg["from"];
                    steps.first++;
                    _peer->_rightIdIndex = _peer->createIndex("right", _peer->_index);
                    _peer->_rightIndexHasValue = true;
                    if (_peer->_leftId == -1) {
                        _peer->_leftId = _peer->_rightId;
                        _peer->_leftIdIndex = _peer->_rightIdIndex;
                        _peer->_leftIndexHasValue = true;
                    }
                }
                else if (_peer->hasHoleLocation("up")) {
                    json message = _peer->buildChannelPayload("up");
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";    
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_upId = msg["from"];
                    steps.second++;
                    _peer->_upIdIndex = _peer->createIndex("up", _peer->_index);
                    _peer->_upIndexHasValue = true;
                    if (_peer->_downId == -1) {
                        _peer->_downId = _peer->_upId;
                        _peer->_downIdIndex = _peer->_upIdIndex;
                        _peer->_downIndexHasValue = true;
                    }
                }
                else if (_peer->hasHoleLocation("left")) {
                    json message = _peer->buildChannelPayload("left");
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_leftId = msg["from"];
                    steps.first--;
                    _peer->_leftIdIndex = _peer->createIndex("left", _peer->_index);
                    _peer->_leftIndexHasValue = true;
                    if (_peer->_rightId == -1) {
                        _peer->_rightId = _peer->_leftId;
                        _peer->_rightIdIndex = _peer->_leftIdIndex;
                        _peer->_rightIndexHasValue = true;
                    }

                }
                else if (_peer->hasHoleLocation("down")) {
                    json message = _peer->buildChannelPayload("down");
                    //std::cerr << "building channel payload for router " << msg["from"]  << "\n";
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_downId = msg["from"];
                    steps.second--;
                    _peer->_downIdIndex = _peer->createIndex("down", _peer->_index);
                    _peer->_downIndexHasValue = true;
                    if (_peer->_upId == -1) {
                        _peer->_upId = _peer->_downId;
                        _peer->_upIdIndex = _peer->_downIdIndex;
                        _peer->_upIndexHasValue = true;
                    }
                }   
            }
            else {
                std::cerr << "SMALLER JOIN\n";
                if ((_peer->_leftId == -1 && _peer->_downId == -1)) { 

                    if (randMod(2) == 0) {
                        json message = _peer->buildChannelPayload("left");
                        //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        _peer->unicastTo(message, msg["from"]);
                        _peer->_leftId = msg["from"];
                        steps.first--;
                        _peer->_leftIdIndex = _peer->createIndex("left", _peer->_index);
                        _peer->_leftIndexHasValue = true;
                        if (_peer->_rightId == -1) {
                            _peer->_rightId = _peer->_leftId;
                            _peer->_rightIdIndex = _peer->_leftIdIndex;
                            _peer->_rightIndexHasValue = true;
                        }
                    }
                    else {
                        json message = _peer->buildChannelPayload("down");
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";
                        _peer->unicastTo(message, msg["from"]);
                        _peer->_downId = msg["from"];
                        steps.second--;
                        _peer->_downIdIndex = _peer->createIndex("down", _peer->_index);
                        _peer->_downIndexHasValue = true;
                        if (_peer->_upId == -1) {
                            _peer->_upId = _peer->_downId;
                            _peer->_upIdIndex = _peer->_downIdIndex;
                            _peer->_upIndexHasValue = true;
                        }
                    }
                }
                else if (_peer->hasHoleLocation("left")) {
                    json message = _peer->buildChannelPayload("left");
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_leftId = msg["from"];
                    steps.first--;
                    _peer->_leftIdIndex = _peer->createIndex("left", _peer->_index);
                    _peer->_leftIndexHasValue = true;
                    if (_peer->_rightId == -1) {
                        _peer->_rightId = _peer->_leftId;
                        _peer->_rightIdIndex = _peer->_leftIdIndex;
                        _peer->_rightIndexHasValue = true;
                    }

                }
                else if (_peer->hasHoleLocation("down")) {
                    json message = _peer->buildChannelPayload("down");
                    //std::cerr << "building channel payload for router " << msg["from"]  << "\n";
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_downId = msg["from"];
                    steps.second--;
                    _peer->_downIdIndex = _peer->createIndex("down", _peer->_index);
                    _peer->_downIndexHasValue = true;
                    if (_peer->_upId == -1) {
                        _peer->_upId = _peer->_downId;
                        _peer->_upIdIndex = _peer->_downIdIndex;
                        _peer->_upIndexHasValue = true;
                    }
                }
                else if (_peer->hasHoleLocation("right")) {
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";
                    json message = _peer->buildChannelPayload("right");
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_rightId = msg["from"];
                    steps.first++;
                    _peer->_rightIdIndex = _peer->createIndex("right", _peer->_index);
                    _peer->_rightIndexHasValue = true;
                    if (_peer->_leftId == -1) {
                        _peer->_leftId = _peer->_rightId;
                        _peer->_leftIdIndex = _peer->_rightIdIndex;
                        _peer->_leftIndexHasValue = true;
                    }
                }
                else if (_peer->hasHoleLocation("up")) {
                    json message = _peer->buildChannelPayload("up");
                    //std::cerr << "building channel payload for router " << msg["from"] << "\n";    
                    _peer->unicastTo(message,msg["from"]);
                    _peer->_upId = msg["from"];
                    steps.second++;
                    _peer->_upIdIndex = _peer->createIndex("up", _peer->_index);
                    _peer->_upIndexHasValue = true;
                    if (_peer->_downId == -1) {
                        _peer->_downId = _peer->_upId;
                        _peer->_downIdIndex = _peer->_upIdIndex;
                        _peer->_downIndexHasValue = true;
                    }
                }

            }
        }
    } 
    else if (msg["type"] == "channel") {
        //std::cerr << "joined peer " <<  _peer->publicId() << " received channel message from peer " << msg["from"] << " to create channel in direction " << msg["location"] << std::endl;
        createChannels(msg);
        json reply = _peer->buildResponsePayload();
        _peer->unicastTo(reply, msg["from"]);
    }
    else if (msg["type"] == "pathFind") {
        //std::cerr << _peer->publicId() << " received path find message from peer " << msg["from"] << " with destination: " << msg["destination"][0] << " " << msg["destination"][1] << std::endl;
        json response = _peer->buildPathFindResponsePayload();
        _peer->unicastTo(response, msg["from"]);
    }
    else if (msg["type"] == "response") {

        //std::cerr << _peer->publicId() << " xxx received response message from peer " << msg["from"] << " with index: " << msg["myIndex"][0] << " " << msg["myIndex"][1] << std::endl;

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
