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
    if (_upId == -1 || (_upIdIndex.second < _index.second && _upIndexHasValue))
        return true;
    else if (_downId == -1 || (_downIdIndex.second > _index.second && _downIndexHasValue))
        return true;
    else if (_rightId == -1 || (_rightIdIndex.first < _index.first && _rightIndexHasValue))
        return true;
    else if (_leftId == -1 || (_leftIdIndex.first > _index.first && _leftIndexHasValue))
        return true;
    else
        return false;
}

bool TorusPeer::hasHoleLocation(std::string location) {
    if (location == "up") {
        return _upId == -1 || (_upIdIndex.second < _index.second && _upIndexHasValue);
    }
    else if (location == "down") {
        return _downId == -1 || (_downIdIndex.second > _index.second && _downIndexHasValue);
    }
    else if (location == "right") {
        return _rightId == -1 || (_rightIdIndex.first < _index.first && _rightIndexHasValue);
    }
    else if (location == "left") {
        return _leftId == -1 || (_leftIdIndex.first > _index.first && _leftIndexHasValue);
    }
    else {
        std::cerr << "invalid location argument for hasHoleLocation\n";
        return false;
    }
}

void TorusPeer::changeState() {
    
    //std::cerr << "CHANGE STATE WAS CALLED\n";
    _state = std::make_unique<JoinedState>(this);

}

// returns indexes of all peers that have a hole
std::vector<std::pair<INDEX,double>> TorusPeer::findHoles(std::vector<Peer*> peers) {

    // typed vector
    std::vector<TorusPeer*> peersWithHole;
    peersWithHole.reserve(peers.size());
    for (auto i : peers) {
        peersWithHole.push_back(static_cast<TorusPeer*>(i));
    }

    
    std::vector<std::pair<INDEX,double>> allHoles;

    for (auto p : peersWithHole) {
        if (p->hasHole() && p->_state->isJoined())
            allHoles.push_back(std::make_pair(p->_index, p->_funds));
    }

    //return closest->_index;

    return allHoles;

}

std::pair<INDEX,bool> TorusPeer::findBestHole() {

    
    if (_allHoles.empty()) {
        std::cerr << "peer: " << publicId() << " found no holes\n";
        return std::make_pair(std::make_pair(0, 0), false);
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

        //std::cerr << "peer: " << publicId() << " found hole with index: " << _allHoles[closest].first.first << " " << _allHoles[closest].first.second << " and funds: " << _allHoles[closest].second << "\n";

        return std::make_pair(_allHoles[closest].first, true);
    }
}

void TorusPeer::initParameters(const std::vector<Peer*>& peers, json parameters) {

    std::cerr << "Initializing parameters for TorusPeer!" << std::endl;

	const std::vector<TorusPeer*> typed = reinterpret_cast<std::vector<TorusPeer*> const&>(peers);

    std::vector<std::pair<INDEX, interfaceId>> allJoined;

    // simulates joining of network
    if (parameters["prebuiltTopology"] == 1) {

        for (auto* p : typed) {
            p->_funds = randMod(parameters["maxFunds"]);
            p->_fundsAvailable = p->_funds;
            p->_bootStrap = peers[0]->publicId();
            p->_state = std::make_unique<NotJoinedState>(p);
            allJoined.push_back(std::make_pair(p->_index, p->publicId()));
            std::cerr << p->_funds << " ";
        }

        typed[0]->_isBootStrap = true;
        typed[0]->changeState();
        typed[0]->_index = {0,0}; typed[0]->_indexHasValue = true;

        // for global knowledge implementation
        // temporary centralized approach
        typed[1]->_readyToJoin = true;

        for (auto* p : typed) {
            p->_allJoined = allJoined;
            p->_allHoles.push_back(std::make_pair(typed[0]->_index, typed[0]->_funds));
        }

    }
    // prebuilt topology
    // primarily used for simulating payments on network 
    else {

        // Assigning indexes and creating channels

        typed[0]->_preBuilt = true;

        int i = 0;
        for (auto* p : typed) {
            // + 1 to avoid randMod(0) in future computation
            p->_funds = randMod(parameters["maxFunds"])+1;
            p->_fundsAvailable = p->_funds;
            p->_bootStrap = peers[0]->publicId();
            p->_index.first = parameters["prebuiltIndexes"][i][0];
            p->_index.second = parameters["prebuiltIndexes"][i][1];
            p->_state = std::make_unique<JoinedState>(p);
            allJoined.push_back(std::make_pair(p->_index, p->publicId()));

            ++i;
        }

        for (auto* p : typed) {
            p->_allJoined = allJoined;

            std::pair<interfaceId, interfaceId> one = p->findSameRC(p->_index, 'c');
            std::pair<interfaceId, interfaceId> two = p->findSameRC(p->_index, 'r');

            p->_downId = one.first;
            p->_upId = one.second;
            p->_leftId = two.first;
            p->_rightId = two.second;
        }

        for (auto* p : typed) {
            p->initChannels();
        }

        // Update channel funds based on other peers 
        // channel funds. Currently only had own balance
        for (auto* p1 : typed) {
            for (auto* p2 : typed) {
                for (auto k : p2->_channels)
                    p1->fundInitHelper(p2->publicId(), k._mine);
            }
        }

        // Creating transactions
        for (int i = 0; i < parameters["paymentNum"]; ++i) {
            int tmp1 = randMod(typed.size());
            int tmp2 = randMod(typed.size());

            // make sure no peer is making payments to itself
            if (tmp1 == tmp2 && tmp2 < typed.size()-1)
                ++tmp2;
            else if (tmp1 == tmp2)
                --tmp2;

            Transaction t;
            if (typed[tmp1]->_funds > typed[tmp2]->_funds)
                t._amount = 1 + (randMod(typed[tmp2]->_funds) / 4);
            else   
                t._amount = 1 + (randMod(typed[tmp1]->_funds) / 4);

            t._source = typed[tmp1]->publicId();
            t._target = typed[tmp2]->publicId();
            typed[tmp1]->_pendingTransactions.push_back(t);
        }

    }


}

// RC stand for Row or Column
// currently centralized, but will become decentralized
std::pair<interfaceId,interfaceId> TorusPeer::findSameRC(INDEX coord, char RC) {

    std::list<std::pair<int, interfaceId>> sameRCGreater;
    std::list<std::pair<int, interfaceId>> sameRCLess;
    
    /*
    interfaceId closestLessPeer = -1;
    double closestLessDist = -1;
    interfaceId closestGreaterPeer = -1;
    double closestGreaterDist = -1;
    */

    std::cerr << publicId() << " is looking for same " << RC << " peers with coord: " << coord.first << " " << coord.second << "\n";

    for (auto i : _allJoined) {
        //std::cerr << "result: " << i.first.first << " " << i.first.second << " id: " << i.second << "\n";
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

            int dist = std::abs(coord.second - i.first.second);
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


INDEX TorusPeer::createIndex(std::string location, INDEX srcIndex) {


    // creating up channel index
    if (location == "up") {
        return std::make_pair(srcIndex.first,srcIndex.second+1);
    }

    // creating down channel index
    else if (location == "down") {
        return std::make_pair(srcIndex.first,srcIndex.second-1);
    }

    // creating right channel index
    else if (location == "right") {
        return std::make_pair(srcIndex.first+1,srcIndex.second);;
    }

    else if (location == "left") {
        return std::make_pair(srcIndex.first-1,srcIndex.second);
    }
    else {
        std::cerr << "invalid location argument for createIndex\n";
        // maybe fix later to non valid index or pair return value with bool
        return {0,0};
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
            //std::cerr << "attempting to add: " << i.first << std::endl;
            if (i.first != -1 && _visited.find(i) == _visited.end()) {
                //std::cerr << "added " << i.first << " to _toVisit\n";
                _toVisit.push_back(i);
            }
        }

        while (!_toVisit.empty()) {
            //std::cerr << "_toVisit was not empty\n";
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

    //std::cerr << "Starting round computation\n";

    Packet packet;

    if (!_state) {
        std::cerr << "bad state pointer\n";
    }

    _state->preComputation();
    
    while (!inStreamEmpty()) {
        packet = popInStream();
        json msg = packet.getMessage();
        //std::cerr << publicId() << " received message from peer " << msg["from"] << " with type: " << msg["type"] << "\n";

        _state->computation(msg);
    }
}

void TorusPeer::initChannels() {_fundsAvailable = _state->distributeFunds(_funds, _fundsAvailable);}

void TorusPeer::transactionFinished() {_state->paymentReset();}

void TorusPeer::fundInitHelper(interfaceId other, double amount) {
    for (auto i : _channels) {
        if (i._otherId == other) {
            i._other = amount;
        }
    }
}

void TorusPeer::updateFunds(interfaceId other, double amount, bool adding) {
    for (auto i : _channels) {
        if (i._otherId == other && !adding) {

            i._mine -= amount;
            i._other += amount;

            std::cerr << publicId() << " funds after " << i._mine << std::endl;

            if (i._mine < 0 || i._other < 0) {
                std::cerr << "ERROR: SOMEBODY WENT INTO NEGATIVE FUNDS IN UPDATEFUNDS\n";
            }

        }
        else if (i._other == other && adding) {

            i._mine += amount;
            i._other -= amount;

            if (i._mine < 0 || i._other < 0) {
                std::cerr << "ERROR: SOMEBODY WENT INTO NEGATIVE FUNDS IN UPDATEFUNDS\n";
            }

        }
    }
}

bool TorusPeer::hasCapacity(interfaceId other, double amount) {
    std::cerr << publicId() << " calling has capacity with other" << other << std::endl;
    for (auto i : _channels) {
        if (i._otherId == other && i._mine > amount) {
            std::cerr << publicId() << " funds before transaction: " << i._mine << std::endl;
            return true;
        }
        else if (i._otherId == other)  {
            std::cerr << publicId() << " only had " << i._mine << ". needed " << amount << std::endl;
        }
    }
    return false;
}


// currently global knowledge implementation using vector of pointers
bool TorusPeer::tryPayment(std::vector<TorusPeer*> peers, Transaction t) {


    std::pair<std::vector<interfaceId>,bool> hasRoute = _state->paymentReady();
    if (hasRoute.second) {

        //std::cerr << "CALLING TRY PAYMENT WITH ROUTE\n";


        std::vector<TorusPeer*> ptrPath;

        for (int i = 0; i < hasRoute.first.size(); ++i) {
            for (auto j : peers) {
                if (j->publicId() == hasRoute.first[i])
                    ptrPath.push_back(j);
            }
        }        

        for (auto p : hasRoute.first) {
            std::cerr << p << " - ";
        }
        std::cerr << std::endl;

        for (int i = ptrPath.size()-1; i > 0; --i) {
            if (ptrPath[i]->hasCapacity(hasRoute.first[i-1], t._amount))
                continue;
            else
                return false;
        }

        for (auto p : hasRoute.first) {
            std::cerr << p << " - ";
        }
        for (auto *p : ptrPath) {
            std::cerr << p->publicId() << " ";
        }
        std::cerr << std::endl;
        std::cerr << "target " << t._target << std::endl;
        std::cerr <<  "source " << t._source << std::endl;
        std::cerr << "PAYMENT SUCCESS\n";

        for (int i = ptrPath.size()-1; i > 0; --i) {
            ptrPath[i]->updateFunds(hasRoute.first[i-1], t._amount, false);
            ptrPath[i-1]->updateFunds(hasRoute.first[i], t._amount, true);
        }

        std::cerr << "PAYMENT SUCCESS " << std::endl;

        LogWriter::pushValue("PaymentSuccessRoute", hasRoute.first);

        return true;
    }
    else {
        return false;
    }

}


void TorusPeer::endOfRound(std::vector<Peer*>& peers) {

    if (peers.empty()) return;

    std::vector<std::pair<INDEX,double>> allHoles = findHoles(peers);

    if (allHoles.empty()) {
        std::cerr << "no holes found\n\n\n";
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
                    j->_searchStartRound = static_cast<int>(RoundManager::currentRound());
                    break;
                }
            }
        }
    }

    /*
    for (auto i : typed) {
        if (i->_state->isJoined())
            std::cerr << "peer " << i->publicId() << " index: " << i->_index.first << " " << i->_index.second << " funds: " << i->_funds << "\n";
    }*/



    if (joinedPeer != nullptr) {
        int roundsTaken = static_cast<int>(RoundManager::currentRound()) - joinedPeer->_searchStartRound; 
        LogWriter::pushValue("latency", roundsTaken);
        LogWriter::pushValue("index", joinedPeer->_index);
        int peersJoined = 0;
        for (auto i : typed) {
            if (i->_state->isJoined()) {
                ++peersJoined;
            }
        }
        LogWriter::pushValue("peersJoined", peersJoined);
    }

    std::vector<std::pair<INDEX, interfaceId>> allJoined;
    for (auto i : typed) {
        if (i->_state->isJoined()) {
            allJoined.push_back(std::make_pair(i->_index, i->publicId()));
        }
    }

    for (auto i : typed) {
        i->_allJoined = allJoined;
    }

    static int confirmedPayments = 0;

    // payment section of end round
    for (auto i : typed) {

        if (!i->_pendingTransactions.empty()) {

            if(i->tryPayment(typed, i->_pendingTransactions.front())) {
                std::cerr << "PAYMENT SUCCESS\n" << std::endl;
                i->_pendingTransactions.pop_front();
                ++confirmedPayments;
            }

        }
    }

    LogWriter::pushValue("throughput",confirmedPayments);

    //std::cerr << "END OF ROUND\n\n\n";

    /*
    for (auto i : typed) {
        if (i->_state->isJoined()) {
            std::cerr << "peer " << i->publicId() << " index: " << i->_index.first << " " << i->_index.second << " has neighbours: " << i->_upId << " " << i->_upIdIndex.first << " " << i->_upIdIndex.second << " | " << i->_downId << " " << i->_downIdIndex.first << " " << i->_downIdIndex.second << " | " << i->_rightId << " " << i->_rightIdIndex.first << " " << i->_rightIdIndex.second << " | " << i->_leftId << " " << i->_leftIdIndex.first << " " << i->_leftIdIndex.second << "\n";
        }
    }*/

}

int TorusPeer::addPaymentChannel(PaymentChannel newChannel) {
    _channels.push_back(newChannel);
    return _channels.size() - 1;
}

json TorusPeer::buildJoinPayload(INDEX destination) const {
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
    //payload["horizontalCapacity"] = ; // to be altered when channels and payments are implemented
    //payload["verticalCapacity"] = ; // to be altered when channels and payments are implemented
    
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
    payload["horizontalSteps"] = _horizontalSteps;
    payload["verticalSteps"] = _verticalSteps;
    payload["channelFunds"] = 0; // to be altered when channels and payments are implemented
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


json TorusPeer::buildPaymentRoutePayload(double amount, bool sender) const {
    json payload;

    if (sender) {
        payload["type"] = "paymentRouteRequest";
        payload["from"] = publicId();
        payload["amount"] = amount;
    }
    else {
        payload["type"] = "paymentRouteResponse";
        payload["from"] = publicId();
        payload["myUp"] = _upId;
        payload["myDown"] = _downId;
        payload["myRight"] = _rightId;
        payload["myLeft"] = _leftId;
        payload["myUpIndex"] = {_upIdIndex.first, _upIdIndex.second};
        payload["myDownIndex"] = {_downIdIndex.first, _downIdIndex.second};
        payload["myLeftIndex"] = {_leftIdIndex.first, _leftIdIndex.second};
        payload["myRightIndex"] = {_rightIdIndex.first, _rightIdIndex.second};
        payload["myIndex"] =  {_index.first, _index.second};
        std::pair<double,double> upCapacity = _state->getFunds("up");
        std::pair<double,double> downCapacity = _state->getFunds("down");
        std::pair<double,double> rightCapacity = _state->getFunds("right");
        std::pair<double,double> leftCapacity = _state->getFunds("left");
        payload["upCapacity"] = {upCapacity.first,upCapacity.second};
        payload["downCapacity"] = {downCapacity.first,downCapacity.second};
        payload["rightCapacity"] = {rightCapacity.first,rightCapacity.second};
        payload["leftCapacity"] = {leftCapacity.first,leftCapacity.second};
    }
    return payload;
}

/*
json TorusPeer::buildPaymentPayload(interfaceId destination, double amount) const {
    json payload;
    payload["type"] = "payment";
    payload["from"] = publicId();
    payload["to"] = destination;
    payload["amount"] = amount;
    return payload;

}*/ // namespace quantas
}