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

void TorusPeer::changeState() {
    
    std::cerr << "CHANGE STATE WAS CALLED\n";
    _state = std::make_unique<JoinedState>(this);

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
        if (p->hasHole() && p->_state->isJoined())
            allHoles.push_back(std::make_pair(p->_index, p->_funds));
    }

    //return closest->_index;

    return allHoles;

}

std::pair<double,double> TorusPeer::findBestHole() {

    
    if (_allHoles.empty()) {
        std::cerr << "peer: " << publicId() << " found no holes\n";
        return {-1,-1};
    }
    else {
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
}

void TorusPeer::initParameters(const std::vector<Peer*>& peers, json parameters) {

    std::cerr << "Initializing parameters for TorusPeer!" << std::endl;

	const std::vector<TorusPeer*> typed = reinterpret_cast<std::vector<TorusPeer*> const&>(peers);

    std::vector<std::pair<std::pair<double,double>, interfaceId>> allJoined;

    for (auto* p : typed) {
        p->_funds = randMod(parameters["maxFunds"]);
        p->_bootStrap = peers[0]->publicId();
        p->_state = std::make_unique<NotJoinedState>(p);
        allJoined.push_back(std::make_pair(p->_index, p->publicId()));
        std::cerr << p->_funds << " ";
    }

    typed[0]->_isBootStrap = true;
    typed[0]->changeState();
    typed[0]->_index = {0.5,0.5};

    // for global knowledge implementation
    // temporary centralized approach
    typed[1]->_readyToJoin = true;

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
        if (RC == 'r' && coord.second == i.first.second && coord != i.first) {
            double dist = std::abs(coord.first - i.first.first);
            if (coord.first > i.first.first) {
                sameRCLess.push_back(std::make_pair(dist, i.second));
            }
            else if (coord.first < i.first.first) {
                sameRCGreater.push_back(std::make_pair(dist, i.second));
            }
        }
        else if (RC == 'c' && coord.first == i.first.first && coord != i.first) {

            double dist = std::abs(coord.second - i.first.second);
            if (coord.second > i.first.second) {
                sameRCLess.push_back(std::make_pair(dist, i.second));
            }
            else if (coord.second < i.first.second) {
                sameRCGreater.push_back(std::make_pair(dist, i.second));
            }
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
    else if (sameRCGreater.size() >= 1 && sameRCLess.size() >= 1) {
        upPeer = sameRCGreater.front().second;
        downPeer = sameRCLess.front().second;
    }

    std::cerr << "FINDSAMERC RESULT" << downPeer << " " << upPeer << "\n";

    return std::make_pair(downPeer,upPeer);
}

INDEX TorusPeer::createIndex(std::string location, INDEX srcIndex, INDEX nextOver) {

    if (location == "up") {
        if (srcIndex.second < nextOver.second && nextOver.second != -1) {
            return std::make_pair(srcIndex.first, (srcIndex.second + nextOver.second) / 2);
        }
        else {
            return std::make_pair(srcIndex.first, (srcIndex.second+1) / 2);
        }
    }
    else if (location == "down") {
        if (srcIndex.second > nextOver.second && nextOver.second != -1) {
            return std::make_pair(srcIndex.first, (srcIndex.second + nextOver.second) / 2);
        }
        else {
            return std::make_pair(srcIndex.first, srcIndex.second / 2);
        }
    }
    else if (location == "right") {
        if (srcIndex.first < nextOver.first && nextOver.first != -1) {
            return std::make_pair((srcIndex.first + nextOver.first) / 2, srcIndex.second);
        }
        else {
            return std::make_pair((srcIndex.first + 1) / 2, srcIndex.second);
        }
    }
    else if (location == "left") {
        if (srcIndex.first > nextOver.first && nextOver.first != -1) {
            return std::make_pair((srcIndex.first + nextOver.first) / 2, srcIndex.second);
        }
        else {
            return std::make_pair(srcIndex.first / 2, srcIndex.second);
        }
    }
    else {
        std::cerr << "invalid location argument for createIndex\n";
        return {-1,-1};
    }
}

// searches and creates channels for peers in same column with holes
void TorusPeer::columnRC(json msg) {
    
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

}

// searches and creates channels for peers in same row with holes
void TorusPeer::rowRC(json msg) {
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
}

// BFS search for destination peer. Uses global knowledge approach
// possiblility to implement more efficient search using known indexes
void TorusPeer::pathFind(json msg) {

    // search start node is bootstrap node
    /*
    if (!_startedSearch) {
        _startedSearch = true;
        _visited.insert(std::make_pair(_bootStrap, std::make_pair(0.5,0.5)));
        //_toVisit.push_back(std::make_pair(_bootStrap, std::make_pair(0.5,0.5)));
    }*/

    if (msg["myIndex"][0] == _dest.first && msg["myIndex"][1] == _dest.second) {
        std::cerr << "EQUALITY TRUE FOR PATH FIND\n";
        if (!_joinSent) {
            json newMsg = buildJoinPayload(_dest);
            unicastTo(newMsg,msg["from"]);
            _joinSent = true;
        }
    }
    else {
        std::cerr << "adding neighbours\n";
        std::vector<std::pair<interfaceId, INDEX>> neighbours;
        neighbours.push_back(std::make_pair(msg["myRight"], msg["myRightIndex"]));
        neighbours.push_back(std::make_pair(msg["myLeft"], msg["myLeftIndex"]));
        neighbours.push_back(std::make_pair(msg["myUp"], msg["myUpIndex"]));
        neighbours.push_back(std::make_pair(msg["myDown"], msg["myDownIndex"]));

        for (auto i : neighbours) {
            std::cerr << "attempting to add: " << i.first << std::endl;
            if (i.first != -1 && _visited.find(i) == _visited.end()) {
                std::cerr << "added " << i.first << " to _toVisit\n";
                _toVisit.push_back(i);
            }
        }

        while (!_toVisit.empty()) {
            std::cerr << "_toVisit was not empty\n";
            std::pair<interfaceId, INDEX> nextPeer = _toVisit.front();
            _toVisit.pop_front();
            _visited.insert(nextPeer);
            json newMsg = buildPathFindPayload(_dest);
            unicastTo(newMsg, nextPeer.first);
        }
    }
}

/*
void TorusPeer::pathFindResponse(json msg) {
    if (msg["destination"][0] == _index.first && msg["destination"][1] == _index.second) {
        //unicastTo(response, msg["from"]);
    }
    else {
        buildRoutePayload(msg["from"]);
    }
}*/


void TorusPeer::performComputation() {

    Packet packet;

    if (!_state) {
        std::cerr << "bad state pointer\n";
    }
    /*
    else {
        if (_state->isJoined())
            std::cerr << "Joined status\n";
        else
            std::cerr << "Not joined status\n";
    }*/


    _state->preComputation();
    
    while (!inStreamEmpty()) {
        packet = popInStream();
        json msg = packet.getMessage();
        std::cerr << publicId() << " received message from peer " << msg["from"] << " with type: " << msg["type"] << "\n";

        _state->computation(msg);
    }
}


void TorusPeer::endOfRound(std::vector<Peer*>& peers) {

    if (peers.empty()) return;

    std::vector<std::pair<std::pair<double,double>,double>> allHoles = findHoles(peers);

    if (allHoles.empty()) {
        std::cerr << "no holes found\n\n\n";
    }

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
        if (i->_state->isJoined() && i->_readyToJoin) {
            std::cerr << "peer: " << i->publicId() << "joined with index: " << i->_index.first << " " << i->_index.second << "\n";

            i->_readyToJoin = false;
            joinedPeer = i;
            for (auto j : typed) {
                if (!j->_state->isJoined()) {
                    j->_readyToJoin = true;
                    j->_startedSearch = static_cast<int>(RoundManager::currentRound());
                    break;
                }
            }
        }
    }

    for (auto i : typed) {
        if (i->_state->isJoined())
            std::cerr << "peer " << i->publicId() << " index: " << i->_index.first << " " << i->_index.second << " funds: " << i->_funds << "\n";
    }



    if (joinedPeer != nullptr) {
        int roundsTaken = static_cast<int>(RoundManager::currentRound()) - joinedPeer->_startedSearch; 
        LogWriter::pushValue("latency", roundsTaken);

        int peersJoined = 0;
        for (auto i : typed) {
            if (i->_state->isJoined()) {
                ++peersJoined;
            }
        }
        LogWriter::pushValue("peersJoined", peersJoined);
    }

    std::vector<std::pair<std::pair<double,double>,interfaceId>> allJoined;
    for (auto i : typed) {
        if (i->_state->isJoined()) {
            allJoined.push_back(std::make_pair(i->_index,i->publicId()));
        }
    }

    for (auto i : typed) {
        i->_allJoined = allJoined;
    }

    for (auto i : typed) {
        if (i->_state->isJoined()) {
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

json TorusPeer::buildPathFindPayload(INDEX destination) const {
    json payload;
    payload["type"] = "pathFind";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["destination"] = destination;
    
    return payload;
}

json TorusPeer::buildPathFindResponsePayload() const {
    json payload;
    payload["type"] = "pathFindResponse";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["myIndex"] = {_index.first, _index.second};
    payload["myLeft"] = _leftId;
    payload["myLeftIndex"] = {_leftIdIndex.first, _leftIdIndex.second};
    payload["myRight"] = _rightId;
    payload["myRightIndex"] = {_rightIdIndex.first, _rightIdIndex.second};
    payload["myUp"] = _upId;
    payload["myUpIndex"] = {_upIdIndex.first, _upIdIndex.second};
    payload["myDown"] = _downId;
    payload["myDownIndex"] = {_downIdIndex.first, _downIdIndex.second};
    
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

json TorusPeer::buildResponsePayload() const {
    json payload;
    payload["type"] = "response";
    payload["from"] = publicId();
    payload["funds"] = _funds;
    payload["myIndex"] = {_index.first, _index.second};
    return payload;
}

} // namespace quantas