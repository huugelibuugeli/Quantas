#include"TorusPeer.hpp"

namespace quantas {

static bool registerTorusPeer = []() {
    return PeerRegistry::registerPeerType(
        "TorusPeer",
        [](interfaceId pubId) { return new TorusPeer(new NetworkInterfaceAbstract(pubId)); });
}();

TorusPeer::TorusPeer(NetworkInterface* networkInterface)
    : Peer(networkInterface) {}

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


    return allHoles;

}

std::pair<INDEX,bool> TorusPeer::findBestHole() {

    if (_allHoles.empty()) {
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

        return std::make_pair(_allHoles[closest].first, true);
    }
}

void TorusPeer::initParameters(const std::vector<Peer*>& peers, json parameters) {

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
        for (auto* p : typed) {
            p->fundInitHelper(typed);
        }

        /*
        for (auto* p : typed) {
            for (auto k : p->_channels)
                std::cerr << k._mine << " " << k._other << "\n";
        }*/

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

void TorusPeer::fundInitHelper(std::vector<TorusPeer*> peers) {

    for (auto p : peers) {
        for (auto& ch : _channels) { 
            if (ch._otherId == p->publicId()) {
                auto it = std::find(p->_channels.begin(), p->_channels.end(), publicId());
                if (it != p->_channels.end()) {
                    ch._other = it->_mine;
                }
            }
        }
    }

}

// RC stand for Row or Column
// currently centralized, but will become decentralized
std::pair<interfaceId,interfaceId> TorusPeer::findSameRC(INDEX coord, char RC) {

    std::list<std::pair<int, interfaceId>> sameRCGreater;
    std::list<std::pair<int, interfaceId>> sameRCLess;
    

    //std::cerr << publicId() << " is looking for same " << RC << " peers with coord: " << coord.first << " " << coord.second << "\n";

    for (auto i : _allJoined) {

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

    if (msg["myIndex"][0] == _dest.first && msg["myIndex"][1] == _dest.second) {
        //std::cerr << "EQUALITY TRUE FOR PATH FIND\n";
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

void TorusPeer::performComputation() {

    Packet packet;

    if (!_state) {
        std::cerr << "bad state pointer\n"; exit(1);
    }

    _state->preComputation();
    
    while (!inStreamEmpty()) {
        packet = popInStream();
        json msg = packet.getMessage();

        _state->computation(msg);
    }

}

void TorusPeer::initChannels() {_fundsAvailable = _state->distributeFunds(_funds, _fundsAvailable);}

void TorusPeer::transactionFinished() {_state->paymentReset();}

void TorusPeer::updateFunds(interfaceId other, double amount, bool adding) {
    for (auto i : _channels) {
        if (i._otherId == other && !adding) {

            i._mine -= amount;
            i._other += amount;

            //std::cerr << publicId() << " funds after " << i._mine << std::endl;

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

    for (auto i : _channels) {
        if (i._otherId == other && i._mine > amount) {
            return true;
        }
        else if (i._otherId == other)  {
            break;
            //std::cerr << publicId() << " only had " << i._mine << ". needed " << amount << std::endl;
        }
    }
    return false;
}


// currently global knowledge implementation using vector of pointers
bool TorusPeer::tryPayment(std::vector<TorusPeer*> peers, Transaction t) {

    std::pair<std::vector<interfaceId>,bool> hasRoute = _state->paymentReady();
    if (hasRoute.second) {

        std::vector<TorusPeer*> ptrPath;

        for (int i = 0; i < hasRoute.first.size(); ++i) {
            for (auto j : peers) {
                if (j->publicId() == hasRoute.first[i])
                    ptrPath.push_back(j);
            }
        }        

        /*
        for (auto p : hasRoute.first) {
            std::cerr << p << " - ";
        }
        std::cerr << std::endl;
        */

        for (int i = ptrPath.size()-1; i > 0; --i) {
            if (ptrPath[i]->hasCapacity(hasRoute.first[i-1], t._amount))
                continue;
            else
                return false;
        }

        /*
        for (auto p : hasRoute.first) {
            std::cerr << p << " - ";
        }
        for (auto *p : ptrPath) {
            std::cerr << p->publicId() << " ";
        }*/

        //std::cerr << std::endl;
        //std::cerr << "target " << t._target << std::endl;
        //std::cerr <<  "source " << t._source << std::endl;
        //std::cerr << "PAYMENT SUCCESS\n";

        for (int i = ptrPath.size()-1; i > 0; --i) {
            ptrPath[i]->updateFunds(hasRoute.first[i-1], t._amount, false);
            ptrPath[i-1]->updateFunds(hasRoute.first[i], t._amount, true);
        }

        //std::cerr << "PAYMENT SUCCESS " << std::endl;

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

    std::vector<TorusPeer*> typed;
    typed.reserve(peers.size());
    for (auto* basePtr : peers) {
        typed.push_back(static_cast<TorusPeer*>(basePtr));
    }

    TorusPeer* joinedPeer = nullptr;
    // checking if next peer can join, since 1 peer at a time approach
    // for temporary centralized approach
    for (auto i : typed) {
        i->_allHoles = allHoles;
        if (i->_state->isJoined() && i->_readyToJoin) {

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

    if (joinedPeer != nullptr) {
        int roundsTaken = static_cast<int>(RoundManager::currentRound()) - joinedPeer->_searchStartRound; 
        LogWriter::pushValue("latency", roundsTaken);
        LogWriter::pushValue("index", joinedPeer->_index);
        LogWriter::pushValue("funds",joinedPeer->_funds);
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

    // payment section of end round

    static int confirmedPayments = 0;

    for (auto i : typed) {
        if (!i->_pendingTransactions.empty() && i->_state->isJoined()) {

            if(i->tryPayment(typed, i->_pendingTransactions.front())) {
                i->_pendingTransactions.pop_front();
                ++confirmedPayments;
            }

        }
    }

    LogWriter::pushValue("throughput",confirmedPayments);


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
        // getFunds returns first mine, second other
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

json TorusPeer::buildRebalanceRoutePayload(bool sender) const {
    json payload;
    
    if (sender) {
        payload["type"] = "rebelanceRouteRequest";
        payload["from"] = publicId();
    }
    else {
        payload["type"] = "rebalanceRouteResponse";
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
        // getFunds returns first mine, second other
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

json TorusPeer::buildRebalanceRequestPayload(double amount, interfaceId rebalanceWith, int index) const {
    json payload;

    payload["type"] = "rebalanceRequest";
    payload["from"] = publicId();
    payload["amount"] = amount;
    payload["rebalanceWith"] = rebalanceWith;
    payload["index"] = index;

    return payload;
}

// searching peer will try multiple routes
// therefore, routeIndex used to indicate rebalancing
// for which route failed
json TorusPeer::buildRebalanceResultPayload(bool success) const {
    json payload;

    if (success) {
        payload["type"] = "rebalanceSuccess";
        payload["from"] = publicId();
    }
    else {
        payload["type"] = "rebalanceFailed";
        payload["from"] = publicId();
    }

    return payload;
}   

}
