#include"TorusPeer.hpp"

namespace quantas {

static bool registerTorusPeer = []() {
    return PeerRegistry::registerPeerType(
        "TorusPeer",
        [](interfaceId pubId) { return new TorusPeer(new NetworkInterfaceAbstract(pubId)); });
}();

TorusPeer::TorusPeer(NetworkInterface* networkInterface)
    : Peer(networkInterface) {std::cerr << "TorusPeer constructor called!" << std::endl;}

// destructor must be defined out-of-line to ensure the vtable is emitted
TorusPeer::~TorusPeer() = default;

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
std::vector<std::pair<std::pair<double,double>,double>> TorusPeer::findHoles(std::vector<Peer*> peers) {

    // typed vector
    std::vector<TorusPeer*> peersWithHole;
    peersWithHole.reserve(peers.size());
    for (auto i : peers) {
        peersWithHole.push_back(static_cast<TorusPeer*>(i));
    }

    
    std::vector<std::pair<std::pair<double,double>,double>> allHoles;

    for (auto p : peersWithHole) {
        if (p->hasHole() && p->_joined)
            allHoles.push_back(std::make_pair(p->_index, p->_funds));
    }

    /*
    TorusPeer* closest = peersWithHole[0];
    double fundGap = std::abs(closest->_funds - _funds);
    for (auto p : peersWithHole) {
        double tmpGap = std::abs(p->_funds - _funds);
        if (tmpGap < fundGap) {
            fundGap = tmpGap;
            closest = p;
        }
    }*/

    //return closest->_index;

    return allHoles;

}

std::pair<double,double> TorusPeer::findBestHole() {

    int closest = 0;
    double fundGap = std::abs(_allHoles[0].second - _funds);
    for (int i = 1; i < _allHoles.size(); ++i) {
        double tmpGap = std::abs(_allHoles[i].second - _funds);
        if (tmpGap < fundGap) {
            fundGap = tmpGap;
            closest = i;
        }
    }

    std::cerr << "peer: " << publicId() << " found hole with index: " << _allHoles[closest].first.first << " " << _allHoles[closest].first.second << " and funds: " << _allHoles[closest].second << "\n";

    return _allHoles[closest].first;
}

void TorusPeer::initParameters(const std::vector<Peer*>& peers, json parameters) {

    std::cerr << "Initializing parameters for TorusPeer!" << std::endl;

	const std::vector<TorusPeer*> typed = reinterpret_cast<std::vector<TorusPeer*> const&>(peers);

    typed[0]->_isBootStrap = true;
    typed[0]->_joined = true;
    typed[0]->_index = {0.5,0.5};

    // for global knowledge implementation
    // temporary centralized approach
    typed[1]->_readyToJoin = true;

    std::vector<std::pair<std::pair<double,double>, interfaceId>> allJoined;

    for (auto* p : typed) {
        p->_funds = randMod(parameters["maxFunds"]);
        p->_bootStrap = peers[0]->publicId();
        allJoined.push_back(std::make_pair(p->_index, p->publicId()));
        std::cerr << p->_funds << " ";
    }

    for (auto* p : typed) {
        p->_allJoined = allJoined;
        p->_allHoles.push_back(std::make_pair(typed[0]->_index, typed[0]->_funds));
    }



}

// RC stand for Row or Column
// currently centralized, but will become decentralized
std::pair<interfaceId,interfaceId> TorusPeer::findSameRC(std::pair<double,double> coord, char RC) {

    std::list<std::pair<double, interfaceId>> sameRCGreater;
    std::list<std::pair<double, interfaceId>> sameRCLess;
    
    /*
    interfaceId closestLessPeer = -1;
    double closestLessDist = -1;
    interfaceId closestGreaterPeer = -1;
    double closestGreaterDist = -1;
    */

    std::cerr << publicId() << " is looking for same " << RC << " peers with coord: " << coord.first << " " << coord.second << "\n";

    for (auto i : _allJoined) {
        std::cerr << "result: " << i.first.first << " " << i.first.second << " id: " << i.second << "\n";
        // searching for row peers
        if (RC == 'r' && coord.second == i.first.second) {
            double dist = std::abs(coord.first - i.first.first);
            if (coord.first > i.first.first) {
                sameRCLess.push_back(std::make_pair(dist, i.second));
            }
            else if (coord.first < i.first.first) {
                sameRCGreater.push_back(std::make_pair(dist, i.second));
            }

            /*
            std::cerr << "thi thithi hi thi thth hit AYAYAYAYYAYA\n";
            if (coord.first > i.first.first) {
                if ((coord.first - i.first.first < closestLessDist) || (closestLessDist == -1)) {
                    closestLessDist = coord.first - i.first.first;
                    closestLessPeer = i.second;
                }
            }
            else if (coord.first < i.first.first) {
                if ((i.first.first - coord.first < closestGreaterDist) || (closestGreaterDist == -1)) {
                    closestGreaterDist = i.first.first - coord.first;
                    closestGreaterPeer = i.second;
                }
            }
                */
        }
        else if (RC == 'c' && coord.first == i.first.first) {

            double dist = std::abs(coord.second - i.first.second);
            if (coord.second > i.first.second) {
                sameRCLess.push_back(std::make_pair(dist, i.second));
            }
            else if (coord.second < i.first.second) {
                sameRCGreater.push_back(std::make_pair(dist, i.second));
            }

            /*
            std::cerr << "THIS WAS TRUEE E E E E E E E E E E\n"; 
            if (coord.second > i.first.second) {
                if ((coord.second - i.first.second < closestLessDist) || (closestLessDist == -1)) {
                    closestLessDist = coord.second - i.first.second;
                    closestLessPeer = i.second;
                }
            }
            else if (coord.second < i.first.second) {
                if ((i.first.second - coord.second < closestGreaterDist) || (closestGreaterDist == -1)) {
                    closestGreaterDist = i.first.second - coord.second;
                    closestGreaterPeer = i.second;
                }
            }
            */
            
        }
    }

    interfaceId upPeer = -1;
    interfaceId downPeer = -1;

    sameRCGreater.sort();
    sameRCLess.sort();
    if (sameRCGreater.size() == 1 && sameRCLess.size() == 0) {
        upPeer = sameRCGreater.front().second;
        downPeer = sameRCGreater.front().second;
    }
    else if (sameRCLess.size() == 1 && sameRCGreater.size() == 0) {
        upPeer = sameRCLess.front().second;
        downPeer = sameRCLess.front().second;
    }
    else if (sameRCGreater.size() > 1 && sameRCLess.size() == 0) {
        upPeer = sameRCGreater.front().second;
        downPeer = sameRCGreater.back().second;
    }
    else if (sameRCLess.size() > 1 && sameRCGreater.size() == 0) {
        upPeer = sameRCLess.back().second;
        downPeer = sameRCLess.front().second;
    }
    else if (sameRCGreater.size() > 1 && sameRCLess.size() > 1) {
        upPeer = sameRCGreater.front().second;
        downPeer = sameRCLess.front().second;
    }

    std::cerr << "FINDSAMERC RESULT" << downPeer << " " << upPeer << "\n";

    return std::make_pair(downPeer,upPeer);
}

std::pair<double, double> TorusPeer::createIndex(std::string location, 
    std::pair<double,double> srcIndex, std::pair<double,double> nextOver)
{

    std::cerr << "src " << srcIndex.first << " " << srcIndex.second << "\n";
    std::cerr << "next " << nextOver.first << " " << nextOver.second << "\n";
    std::cerr << "location: " << location << "\n";

    std::pair<double,double> index;

    if (location == "up" || location == "down") {
        index.first = srcIndex.first;

        // ensures that index for down is always less
        // and index for up is always more 
        if (srcIndex != nextOver && nextOver.second != -1)
            index.second = (srcIndex.second + nextOver.second) / 2;
        else if (location == "up")
            index.second = (srcIndex.second + 1) / 2;
        else
            index.second = srcIndex.second / 2;
    }
    else {
        index.second = srcIndex.second;

        if (srcIndex != nextOver && nextOver.first != -1)
            index.first = (srcIndex.first + nextOver.first) / 2;
        else if (location == "right")
            index.first = (srcIndex.first + 1) / 2;
        else
            index.first = srcIndex.first / 2;
    }

    std::cerr << publicId() << " created index: " << index.first << " " << index.second << "\n";

    return index;
}

void TorusPeer::createChannels(json msg) {

    std::cerr << "Received channel message from peer " << msg["from"] << " to create channel in direction " << msg["location"] << std::endl;

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
                _downId = msg["from"];
                _upIdIndex = srcIndex;
                _downIdIndex = srcIndex;
            }

            _index = createIndex("up",srcIndex,_upIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index, 'r');

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
            std::cerr << "made it here\n";
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
                _upId = msg["from"];
                _downIdIndex = srcIndex;
                _upIdIndex = srcIndex;
            }

            _index = createIndex("down", srcIndex, _downIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index, 'r');

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
                _rightIdIndex.first = msg["myRightIndex"][0];
                _rightIdIndex.second = msg["myRightIndex"][1];
                json newChannelMsg = buildChannelPayload("right");
                unicastTo(newChannelMsg, _rightId);
            }
            else {
                _leftId = msg["from"];
                _rightId = msg["from"];
                _leftIdIndex = srcIndex;
                _rightIdIndex = srcIndex;
            }

            _index = createIndex("right", srcIndex, _rightIdIndex);

            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index, 'c');

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
                _leftIdIndex.first = msg["myLeftIndex"][0];
                _leftIdIndex.second = msg["myLeftIndex"][1];
                json newChannelMsg = buildChannelPayload("left");
                unicastTo(newChannelMsg, _leftId);
            }
            else {
                _leftId = msg["from"];
                _rightId = msg["from"];
                _leftIdIndex = srcIndex;
                _rightIdIndex = srcIndex;
            }

            _index = createIndex("left", srcIndex, _leftIdIndex);
            std::pair<interfaceId,interfaceId> sameRC = findSameRC(_index, 'c');

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
            _downId = msg["from"];
            _downIdIndex = srcIndex;
        }
        else if (msg["location"] == "down") {
            _upId = msg["from"];
            _upIdIndex = srcIndex;
        }
        else if (msg["location"] == "right") {
            _leftId = msg["from"];
            _leftIdIndex = srcIndex;
        }
        else if (msg["location"] == "left") {
            _rightId = msg["from"];
            _rightIdIndex = srcIndex;
        }
    }
}

void TorusPeer::computationNotJoined(json msg) {

    if (msg["type"] == "route") {
        std::cerr << publicId() << " received route with next: " << msg["nextPeer"] << "\n";
        if (_dest.first == -1)  {
            if ((msg["funds"] > _funds && _lastMessage["funds"] < _funds) ||
                (msg["funds"] < _funds && _lastMessage["funds"] > _funds)) {
                // sets dest to node that peer wants to join
                _dest = {msg["myIndex"][0],msg["myIndex"][1]};
                json newJoinMsg = buildJoinPayload(_dest);
                std::cerr << publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << _dest.first << " " << _dest.second << "\n";
                unicastTo(newJoinMsg, msg["from"]);
            }
            else {
                json message = buildJoinPayload({-1,-1});
                std::cerr << publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << -1 << " " << -1 << "\n";
                unicastTo(message, msg["nextPeer"]);
            }
        }
        else {
            json message = buildJoinPayload(_dest);
            std::cerr << publicId() << " is sending join message to " << msg["nextPeer"] << " with dest: " << _dest.first << " " << _dest.second << "\n";
            unicastTo(message, msg["nextPeer"]);
        }
    }
    else if (msg["type"] == "channel")
        createChannels(msg);

}

void TorusPeer::computationJoined(json msg) {

    if (msg["type"] == "join") {
        if (msg["destination"][0] != -1) {

            if (_index.first < msg["destination"][0] && _rightId != -1 && _rightIdIndex.first > _index.first) {
                json message = buildRoutePayload(_rightId);
                unicastTo(message, msg["from"]);
            }
            else if (_index.first > msg["destination"][0] && _leftId != -1 && _leftIdIndex.first < _index.first) {
                json message = buildRoutePayload(_leftId);
                unicastTo(message, msg["from"]);
            }                    
            else if (_index.second < msg["destination"][1] && _upId != -1 && _upIdIndex.second > _index.second) {
                json message = buildRoutePayload(_upId);
                unicastTo(message, msg["from"]);
            }
            else if (_index.second > msg["destination"][1] && _downId != -1 && _downIdIndex.second < _index.second) {
                json message = buildRoutePayload(_downId);
                unicastTo(message, msg["from"]);
            }
            else if (_index.first == msg["destination"][0] && _index.second == msg["destination"][1]) {
                if (msg["funds"] > _funds) {
                    if ((_rightId == -1 && _upId == -1 )|| (_rightId != -1 && _upId != -1)) { 

                        if (randMod(2) == 0) {
                            json message = buildChannelPayload("right");
                            unicastTo(message, msg["from"]);
                            _rightId = msg["from"];
                            _rightIdIndex = createIndex("right", _index, _rightIdIndex);
                            if (_leftId == -1) {
                                _leftId = _rightId;
                                _leftIdIndex = _rightIdIndex;
                            }   
                        }
                        else {
                            json message = buildChannelPayload("up");
                            unicastTo(message, msg["from"]);
                            _upId = msg["from"];
                            _upIdIndex = createIndex("up", _index, _upIdIndex);
                            if (_downId == -1) {
                                _downId = _upId;
                                _downIdIndex = _upIdIndex;
                            }
                        }
                    }
                    else if (_rightId == -1) {
                        json message = buildChannelPayload("right");
                        unicastTo(message,msg["from"]);
                        _rightId = msg["from"];
                        _rightIdIndex = createIndex("right", _index, _rightIdIndex);
                        if (_leftId == -1) {
                            _leftId = _rightId;
                            _leftIdIndex = _rightIdIndex;
                        }
                    }
                    else if (_upId == -1) {
                        json message = buildChannelPayload("up");
                        unicastTo(message,msg["from"]);
                        _upId = msg["from"];
                        _upIdIndex = createIndex("up", _index, _upIdIndex);
                        if (_downId == -1) {
                            _downId = _upId;
                            _downIdIndex = _upIdIndex;
                        }
                    }   
                }
                else {
                    if ((_leftId == -1 && _downId == -1 )|| (_leftId != -1 && _downId != -1)) { 

                        if (randMod(2) == 0) {
                            json message = buildChannelPayload("left");
                            unicastTo(message, msg["from"]);
                            _leftId = msg["from"];
                            _leftIdIndex = createIndex("left", _index, _leftIdIndex);
                            if (_rightId == -1) {
                                _rightId = _leftId;
                                _rightIdIndex = _leftIdIndex;
                            }
                        }
                        else {
                            json message = buildChannelPayload("down");
                            unicastTo(message, msg["from"]);
                            _downId = msg["from"];
                            _downIdIndex = createIndex("down", _index, _downIdIndex);
                            if (_upId == -1) {
                                _upId = _downId;
                                _upIdIndex = _downIdIndex;
                            }
                        }
                    }
                    else if (_leftId == -1) {
                        json message = buildChannelPayload("left");
                        unicastTo(message,msg["from"]);
                        _leftId = msg["from"];
                        _leftIdIndex = createIndex("left", _index, _leftIdIndex);
                        if (_rightId == -1) {
                            _rightId = _leftId;
                            _rightIdIndex = _leftIdIndex;
                        }

                    }
                    else if (_downId == -1) {
                        json message = buildChannelPayload("down");
                        unicastTo(message,msg["from"]);
                        _downId = msg["from"];
                        _downIdIndex = createIndex("down", _index, _downIdIndex);
                        if (_upId == -1) {
                            _upId = _downId;
                            _upIdIndex = _downIdIndex;
                        }
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
        json reply = buildResponsePayload();
        unicastTo(reply, msg["from"]);
    }
    else if (msg["type"] == "response") {

        std::cerr << "\n\nTHIS HAPPENED\n\n";

        if (msg["from"] == _rightId) {
            if (_rightIdIndex.first == -1)
                _rightIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _upId) {
            if (_upIdIndex.first == -1)
                _upIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _leftId) {
            if (_leftIdIndex.first == -1)
                _leftIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _downId) {
            if (_downIdIndex.first == -1)
                _downIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
    }
}


void TorusPeer::performComputation() {


    if (!_joined && _readyToJoin) {
        // returns index of peer with hole that 
        // has closest funds to caller
        //std::cerr << "peer: " << publicId() << " is looking for hole with closest funds\n";
        std::pair<double,double> newDest = findBestHole();
        if (newDest != _dest)
            std::cerr << "peer: " << publicId() << " found hole with index: " << newDest.first << " " << newDest.second << "\n";

        if (newDest.first == -1) {
            _dest = {-1,-1};
            json message = buildJoinPayload(_dest);
            std::cerr << publicId() << " is sending bootstrap join message to " << _bootStrap << " with dest: " << _dest.first << " " << _dest.second << "\n";
            unicastTo(message, _bootStrap);
        }
        else if (_dest.first == -1 && newDest.first != -1) {
            _dest = newDest;
            json message = buildJoinPayload(_dest);
            std::cerr << publicId() << " is sending bootstrap join message to " << _bootStrap << " with dest: " << _dest.first << " " << _dest.second << "\n";
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

    std::vector<std::pair<std::pair<double,double>,double>> allHoles = findHoles(peers);
    for (auto i : allHoles) {
        std::cerr << "hole index: " << i.first.first << " " << i.first.second << " funds: " << i.second << "\n";
    }

    std::vector<TorusPeer*> typed;
    typed.reserve(peers.size());
    for (auto* basePtr : peers) {
        typed.push_back(static_cast<TorusPeer*>(basePtr));
    }


    TorusPeer* joinedPeer = nullptr;
    // checking if next peer can join
    // for temporary centralized approach
    for (auto i : typed) {
        i->_allHoles = allHoles;
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

    for (auto i : typed) {
        if (i->_joined)
            std::cerr << "peer " << i->publicId() << " index: " << i->_index.first << " " << i->_index.second << " funds: " << i->_funds << "\n";
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

    std::vector<std::pair<std::pair<double,double>,interfaceId>> allJoined;
    for (auto i : typed) {
        if (i->_joined) {
            allJoined.push_back(std::make_pair(i->_index,i->publicId()));
        }
    }

    for (auto i : typed) {
        i->_allJoined = allJoined;
    }

    for (auto i : typed) {
        if (i->_joined) {
            std::cerr << "peer " << i->publicId() << " index: " << i->_index.first << " " << i->_index.second << " has neighbours: " << i->_upId << " " << i->_upIdIndex.first << " " << i->_upIdIndex.second << " | " << i->_downId << " " << i->_downIdIndex.first << " " << i->_downIdIndex.second << " | " << i->_rightId << " " << i->_rightIdIndex.first << " " << i->_rightIdIndex.second << " | " << i->_leftId << " " << i->_leftIdIndex.first << " " << i->_leftIdIndex.second << "\n";
        }
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

json TorusPeer::buildChannelPayload(std::string location) const {
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

/*
json TorusPeer::buildQueryPayload() const {
    json payload;
    payload["type"] = "query";
    payload["from"] = publicId();
    return payload;
}*/

json TorusPeer::buildResponsePayload() const {
    json payload;
    payload["type"] = "response";
    payload["from"] = publicId();
    payload["funds"] = _funds;
    payload["myIndex"] = {_index.first, _index.second};
    return payload;
}

} // namespace quantas