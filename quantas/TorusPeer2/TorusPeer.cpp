#include"TorusPeer.hpp"

static bool registerTorusPeer = []() {
    return PeerRegistry::registerPeerType(
        "TorusPeer",
        [](interfaceId pubId) { return new TorusPeer(new NetworkInterfaceAbstract(pubId)); });
}();

bool TorusPeer::hasHole() {
    if (_upId == -1)
        return true;
    else if (_downId == -1)
        return true;
    else if (_rightId == -1)
        return true;
    else if (_leftId == -1)
        return true;
    else
        return false;
}

// returns indexes of all peers that have a hole
std::vector<std::pair<double,double>>& TorusPeer::findHoles(std::set<Peer*> peers) {

    std::vector<std::pair<double,double>> peersWithHole;
    
    for (auto p : peers) {
        if (p->hasHole() && p->_joined)
            peersWithHole.push_back(p->_index);
    }

    return peersWithHole;

}

void TorusPeer::initParameters(std::vector<Peer*>& peers, json parameters) {

	const vector<TorusPeer*> peers = reinterpret_cast<vector<TorusPeer*> const&>(peers);

    peers[0]->_bootStrap = true;
    peers[0]->_joined = true;
    peers[0]->_index = {0,0};

    // for global knowledge implementation
    // temporary centralized approach
    peers[1]->_readyToJoin = true;


    for (auto p : peers) {
        p->_funds = randMod(parameters["maxFunds"]);
        p->_bootStrap = peers[0]->publicId();
        std::cerr << p->_funds << " ";
    }

}

// RC stand for Row or Column
// currently centralized, but will become decentralized
std::pair<interfaceId,interfaceId> TorusPeer::findSameRC(double coord, char RC) {

    interfaceId closestLessPeer = -1;
    interfaceId closestGreaterPeer = -1;

    for (auto i : _allJoined) {

        // searching for row peers
        if (RC == 'r' && coord == i.first.second) {
            if (coord > i.first.second) {
                if ((coord - i.first.second < closestLessPeer) || (closestLessPeer == -1)) {
                    closestLessPeer = i.second;
                }
            }
            else if (coord < i.first.second) {
                if ((i.first.second - coord < closestGreaterPeer) || (closestGreaterPeer == -1)) {
                    closestGreaterPeer = i.second;
                }
            }
        }
        else if (RC == 'c' && coord == i.first.first) {
            if (coord > i.first.first) {
                if ((coord - i.first.first < closestLessPeer) || (closestGreaterPeer == -1)) {
                    closestLessPeer = i.second;
                }
            }
            else if (coord < i.first.first) {
                if ((i.first.first - coord < closestGreaterPeer) || (closestGreaterPeer == -1)) {
                    closestGreaterPeer = i.second;
                }
            }
        }
    }

    return std::make_pair(closestLessPeer,closestGreaterPeer);
}

std::pair<double, double> TorusPeer::createIndex(std::string location, 
    std::pair<double,double> srcIndex, std::pair<double,double> nextOver)
{
    std::pair<double,double> index;

    if (location == "up" || location == "down") {
        index.first = srcIndex.first;

        // ensures that index for down is always less
        // and index for up is always more 
        if (srcIndex != nextOver)
            index.second = (srcIndex.second + nextOver.second) / 2;
        else if (location == "up")
            index.second = (srcIndex.second + 1) / 2;
        else
            index.second = srcIndex / 2;
    }
    else {
        index.second = srcIndex.second;

        if (srcIndex != nextOver)
            index.first = (srcIndex.first + nextOver.first) / 2;
        else if (location == "right")
            index.second = (srcIndex.second + 1) / 2;
        else
            index.second = srcIndex / 2;
    }

    return index;
}

bool TorusPeer::createChannels(json msg) {

    std::pair<double,double> srcIndex;
    srcIndex.first = msg["myIndex"][0];
    srcIndex.second = msg["myIndex"][1];

    if (!_joined) {

        // joining above source
        if (msg["location"] == "up") {
            _downId = msg["from"];
            _downIdIndex = srcIndex;

            if (msg["myUp"] != -1) {
                _upId = msg["myUp"];
                _upIdIndex.first = msg["myUpIndex"][0];
                _upIdIndex.second = msg["myUpIndex"][1];
                json newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg, _upId);
            }
            else {
                _upId = msg["from"];
                _upIdIndex = srcIndex;
            }

            _index = createIndex("up",srcIndex,_upIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index.second, 'r');

            if (sameRC.first != -1 && sameRC.second != -1) {
                _leftId = sameRC.first;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.second;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }
            else if (sameRC.first == -1 && sameRC.second != -1) {
                _leftId = sameRC.second;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.second;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }
            else if (sameRC.first != -1 && sameRC.second == -1) {
                _leftId = sameRC.first;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.first;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }            


            _joined = true;
        }

        // joining below source
        else if (msg["location"] == "down") {
            _upId = msg["from"]; // interfaceId
            _upIdIndex = srcIndex;

            if (msg["myDown"] != -1) {
                _downId = msg["myDown"];
                _downIdIndex.first = msg["myDownIndex"][0];
                _downIdIndex.second = msg["myDownIndex"][1];
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);
            }
            else {
                _downId = msg["from"];
                _downIdIndex = srcIndex;
            }

            _index = createIndex("down", srcIndex, _downIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index.second, 'r');

            if (sameRC.first != -1 && sameRC.second != -1) {
                _leftId = sameRC.first;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.second;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }
            else if (sameRC.first == -1 && sameRC.second != -1) {
                _leftId = sameRC.second;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.second;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }
            else if (sameRC.first != -1 && sameRC.second == -1) {
                _leftId = sameRC.first;
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg,_leftId);

                _rightId = sameRC.first;
                newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg,_rightId);
            }

            _joined = true;
        }

        // joining to the right of source
        else if (msg["location"] == "right") {
            _leftId = msg["from"];

            if (msg["myRight"] != -1) {
                _rightId = msg["myRight"];
                _rightIdIndex.first = msg["myIndex"][0];
                _rightIdIndex.second = msg["myIndex"][1];
                json newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg, _rightId);
            }
            else {
                _rightId = msg["from"];
                _rightIdIndex = srcIndex;
            }

            _index = createIndex("right", srcIndex, _rightIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index.first, 'c');

            if (sameRC.first != -1 && sameRC.second != -1) {
                _downId = sameRC.first;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.second;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }
            else if (sameRC.first == -1 && sameRC.second != -1) {
                _downId = sameRC.second;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.second;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }
            else if (sameRC.first != -1 && sameRC.second == -1) {
                _downId = sameRC.first;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.first;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }

            _joined = true;
        }
        else if (msg["location"] == "left") {
            _rightId = msg["from"];

            if (msg["myLeft"] != -1) {
                _leftId = msg["myLeft"];
                _leftIdIndex.first = msg["myIndex"][0];
                _leftIdIndex.second = msg["myIndex"][1];
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg, _leftId);
            }
            else {
                _leftId = msg["from"];
                _leftIdIndex = srcIndex;
            }

            _index = createIndex("right", srcIndex, _rightIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index.first, 'c');

            if (sameRC.first != -1 && sameRC.second != -1) {
                _downId = sameRC.first;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.second;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }
            else if (sameRC.first == -1 && sameRC.second != -1) {
                _downId = sameRC.second;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.second;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }
            else if (sameRC.first != -1 && sameRC.second == -1) {
                _downId = sameRC.first;
                json newChannelMsg = buildChannelPayload("down");
                unicastTo(newChannelMsg,_downId);

                _upId = sameRC.first;
                newChannelMsg = buildChannelPayload("up");
                unicastTo(newChannelMsg,_upId);
            }

            _joined = true;
        }
    }
    // already joined
    else {
        if (msg["location"] == "up") {
            _downId = srcIndex;
            _downIdIndex.first = msg["myIndex"][0];
            _downIdIndex.second = msg["myIndex"][1];
        }
        else if (msg["location"] == "down") {
            _upId = srcIndex;
            _upIdIndex.first = msg["myIndex"][0];
            _upIdIndex.second = msg["myIndex"][1];
        }
        else if (msg["location"] == "right") {
            _leftId = srcIndex;
            _leftIdIndex.first = msg["myIndex"][0];
            _leftIdIndex.second = msg["myIndex"][1];
        }
        else if (msg["location"] == "left") {
            _rightId = srcIndex;
            _rightIdIndex.first = msg["myIndex"][0];
            _rightIdIndex.second = msg["myIndex"][1];
        }
    }
}

void TorusPeer::computationNotJoined(json msg) {

    if (msg["type"] == "route") {
        if (_dest.first == -1)  {
            if ((msg["funds"] > _funds && _lastMessage["funds"] < _funds ||
                 msg["funds"] < _funds && _lastMessage["funds"] > _funds) {
                // sets dest to node that peer wants to join
                _dest = {msg[myIndex][0],msg[myIndex][1]};
                json newJoinMsg = buildJoinPayload(_dest);
                unicastTo(newJoinMsg, msg["from"]);
            }
            else {
                json message = buildJoinPayload({-1,-1});
                unicastTo(message, msg["nextPeer"]);
            }
        }
        else {
            json message = buildJoinPayload(_dest);
            unicastTo(message, msg["nextPeer"]);
        }
    }
    else if (msg["type"] == "channel")
        createChannels(msg);

}

void TorusPeer::computationJoined(json msg) {

    if (msg["type"] == "join") {
        if (msg["destination"][0] != -1) {

            if (_index.first < msg["destination"][0]) {
                if (_rightId != -1) {
                    json message = buildRoutePayload(_rightId);
                    unicastTo(message, msg["from"]);
                }
            }
            else if (_index.first > msg["destination"][0]) {
                if (_leftId != -1) {
                    json message = buildRoutePayload(_leftId);
                    unicastTo(message, msg["from"]);
                }
            }                    
            else if (_index.second < msg["destination"][1]) {
                if (_upId != -1) {
                    json message = buildRoutePayload(_upId);
                    unicastTo(message, msg["from"]);
                }
            }
            else if (_index.second > msg["destination"][1]) {
                if (_downId != -1) {
                    json message = buildRoutePayload(_downId);
                    unicastTo(message, msg["from"]);
                }
            }
            else if (_index.first == msg["destination"][0] && _index.second == msg["destination"][1]) {
                if (msg["funds"] > _funds) {
                    if ((_rightId == -1 && _upId == -1 )|| (_rightId != -1 && _leftId != -1)) { 

                        if (randMod(2) == 0) {
                            json message = buildChannelPayload("right");
                            unicastTo(message, target);
                        }
                        else {
                            json message = buildChannelPayload("up");
                            unicastTo(message, target);
                        }
                    }
                    else if (_rightId == -1) {
                        json message = buildChannelPayload("right");
                        unicast(message,msg["from"]);
                    }
                    else if (_upId == -1) {
                        json message = buildChannelPayload("up");
                    }   
                }
                else {
                    if ((_leftId == -1 && _downId == -1 )|| (_leftId != -1 && _downId != -1)) { 

                        if (randMod(2) == 0) {
                            json message = buildChannelPayload("left");
                            unicastTo(message, target);
                        }
                        else {
                            json message = buildChannelPayload("down");
                            unicastTo(message, target);
                        }
                    }
                    else if (_leftId == -1) {
                        json message = buildChannelPayload("left");
                        unicast(message,msg["from"]);
                    }
                    else if (_downId == -1) {
                        json message = buildChannelPayload("down");
                    }   
                }
            }
        }
        else if (msg["destination"][0] == -1) {
            if (_funds >= msg["funds"]) {
                if (randMod(2) == 1) {
                    json newRouteMsg = buildRoutePayload(_rightId);
                    unicastTo(newRouteMsg,msg["from"]);
                }
                else {
                    json newRouteMsg = buildRoutePayload(_upId);
                    unicastTo(newRouteMsg,msg["from"]);
                }
            }
            else {
                if (randMod(2) == 1) {
                    json newRouteMsg = buildRoutePayload(_leftId);
                    unicastTo(newRouteMsg,msg["from"]);
                }
                else {
                    json newRouteMsg = buildRoutePayload(_downId);
                    unicastTo(newRouteMsg,msg["from"]);
                }
            }
        }
    }
    else if (msg["type"] == "channel") {
        createChannels(msg);
    }
}


void TorusPeer::performComputation() {

    if (!_joined && _readyToJoin) {
        std::vector<*Peer> peersWithHole = findHoles(neighbors());

        if (_dest.first == -1 && !peersWithHole.empty()) {
        
            // finds best hole for peer based on funds
            *Peer closest = peersWithHole[0];
            double fundGap = std::abs(closest->_funds - _funds);
            for (p : peersWithHole) {
                double tmpGap = std::abs(p->_funds - _funds);
                if (tmpGap < fundGap) {
                    fundGap = tmpGap;
                    closest = *p;
                }
            }
            _dest = closest->_index;
            json message = buildJoinPayload(_dest);
            unicastTo(message, _bootStrap);
        }
        if (peersWithHole.empty()) {
            _dest = {-1,-1};
            json message = buildJoinPayload(_dest);
            unicastTo(message, _bootStrap);
        }
    }
    
    while (!inStreamEmpty()) {
        Packet packet = popInStream();
        json msg = packet.getMessage();

        if (!_joined) {
            computationNotJoined(msg);
        }   
        else {
            computationJoined(msg);
        }
    }
}


void TorusPeer::endOfRound(std::vector<Peer*>& peers) {

    if (peers.empty()) return;

    std::vector<TorusPeer*> typed;
    typed.reserve(peers.size());
    for (auto* basePtr : peers) {
        typed.push_back(static_cast<TorusPeer*>(basePtr));
    }


    TorusPeer* joinedPeer = nullptr;
    // checking if next peer can join
    // for temporary centralized approach
    for (auto i : typed) {
        if (i->_joined && i->_readyToJoin) {
            std::cerr << "peer: " << i->publicId() << "joined with index: " << i->_index.first << " " << i->_index.second << "\n";

            i->_readyToJoin = false;
            joinedPeer = i;
            for (auto j : typed) {
                if (!j->_joined) {
                    j->_readyToJoin = true;
                    j->_startedSearch = static_cast<int>(RoundManager::currentRound());
                    break;
                }
            }
        }
    }



    if (joinedPeer != nullptr) {
        int roundsTaken = static_cast<int>(RoundManager::currentRound()) - joinedPeer->_startedSearch; 
        LogWriter::pushValue("latency", roundsTaken);

        int peersJoined = 0;
        for (auto i : typed) {
            if (i->_joined) {
                ++peersJoined;
            }
        }
        LogWriter::pushValue("peersJoined", peersJoined);
    }

    std::vector<std::pair<double,double>,interfaceId> allJoined;
    for (auto i : typed) {
        if (i->_joined)
            allJoined.push_back(i);
    }

    for (auto i : typed) {
        i->_allJoined = allJoined;
    }

}

json TorusPeer::buildJoinPayload(std::pair<double,double> destination) const {
    json payload;
    payload["type"] = "join";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["funds"] = _funds;
    payload["destination"] = destination;
    
    return payload;
}

json TorusPeer::buildRoutePayload(interfaceId nextQuery) const {
    json payload;
    payload["type"] = "route";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["funds"] = _funds;
    payload["myIndex"] = {_index.first, _index.second};
    payload["nextPeer"] = nextQuery;
    
    return payload;
}

json TorusPeer::buildChannelPayload(std::string location) {
    json payload;
    payload["type"] = "channel";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["location"] = location;
    payload["myUp"] = _upId;
    payload["myDown"] = _downId;
    payload["myRight"] = _rightId;
    payload["myLeft"] = _leftId;
    payload["myUpIndex"] = {_upIdIndex.first, _upIdIndex.second};
    payload["myDownIndex"] = {_downIdIndex.first, _downIdIndex.second};
    payload["myLeftIndex"] = {_leftIdIndex.first, _leftIdIndex.second};
    payload["myRightIndex"] = {_rightIdIndex.first, _rightIdIndex.second};
    payload["myIndex"] =  {_index.first, _index.second};
    return payload;
}