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
        if (p->hasHole() && p->_state->isJoined())
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
        p->_state = new NotJoinedState(p);
        allJoined.push_back(std::make_pair(p->_index, p->publicId()));
        std::cerr << p->_funds << " ";
    }

    typed[0]->_isBootStrap = true;
    typed[0]->changeState(new JoinedState(typed[0]));
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
        else if (RC == 'c' && coord.first == i.first.first && coord != i.first) {

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
    else if (sameRCGreater.size() >= 1 && sameRCLess.size() >= 1) {
        upPeer = sameRCGreater.front().second;
        downPeer = sameRCLess.front().second;
    }

    std::cerr << "FINDSAMERC RESULT" << downPeer << " " << upPeer << "\n";

    return std::make_pair(downPeer,upPeer);
}

/*
std::pair<double, double> TorusPeer::createIndex(std::string location, 
    INDEX srcIndex, INDEX nextOver)
{

    std::cerr << "src " << srcIndex.first << " " << srcIndex.second << "\n";
    std::cerr << "next " << nextOver.first << " " << nextOver.second << "\n";
    std::cerr << "location: " << location << "\n";

    std::pair<double,double> index;

    std::pair<interfaceId,interfaceId> nilPair = {-1,-1};

    if (location == "up" || location == "down") {
        index.first = srcIndex.first;

        // ensures that index for down is always less
        // and index for up is always more 
        //if (srcIndex != nextOver && nextOver.second != -1)
        //    index.second = (srcIndex.second + nextOver.second) / 2;

        // build up case
        if (location == "up") {

            if (srcIndex != nextOver && nextOver.second != -1) {
                if (srcIndex.second < nextOver.second) {
                    index.second = (srcIndex.second + nextOver.second) / 2;
                }
                else {
                    index.second = (srcIndex.second + 1) / 2;
                }
            }

            else {
            //index.second = (srcIndex.second + 1) / 2;

            INDEX tmpIndex1 = {(srcIndex.first + 1)/2, srcIndex.second};
            INDEX tmpIndex2 = {srcIndex.first / 2, srcIndex.second};
            std::pair<interfaceId,interfaceId> sameRC1 = findSameRC(tmpIndex1, 'c');
            std::pair<interfaceId,interfaceId> sameRC2 = findSameRC(tmpIndex2, 'c');
            if (sameRC1 == nilPair && sameRC2 == nilPair) {
                index.second = (srcIndex.second + 1) / 2;
            }
            else {
                // global knowledge approach
                INDEX closestNextAbove = {-1,-1};
                INDEX closestNextBelow = {-1,-1};
                for (auto i : _allJoined) {
                    if (i.second == sameRC1.second) {
                        closestNextAbove = i.first;
                        break;
                    }
                }
                for (auto i : _allJoined) {
                    if (i.second == sameRC2.second) {
                        closestNextBelow = i.first;
                        break;
                    }
                }
                if (closestNextBelow.second > srcIndex.second && closestNextAbove.second > srcIndex.second) {
                    if ((closestNextAbove.first - srcIndex.first) < (srcIndex.first - closestNextBelow.first)) {
                        index.second = closestNextAbove.second;
                    }
                    else {
                        index.second = closestNextBelow.second;
                    }
                }
                else if (closestNextBelow.second != -1) {
                    if (closestNextBelow.second > srcIndex.second) {
                        index.second = closestNextBelow.second;
                    }
                    else {
                        index.second = (srcIndex.second + 1) / 2;
                    }
                }
                else if (closestNextAbove.second != -1) {
                    if (closestNextAbove.second > srcIndex.second) {
                        index.second = closestNextAbove.second;
                    }
                    else {
                        index.second = (srcIndex.second + 1) / 2;
                    }
                }
                else {
                    index.second = (srcIndex.second + 1) / 2;
                }
            }
        }

        // build down case
        else {
            INDEX tmpIndex1 = {(srcIndex.first + 1) / 2, srcIndex.second};
            INDEX tmpIndex2 = {srcIndex.first / 2, srcIndex.second};
            std::pair<interfaceId,interfaceId> sameRC1 = findSameRC(tmpIndex1, 'c');
            std::pair<interfaceId,interfaceId> sameRC2 = findSameRC(tmpIndex2, 'c');
            if (sameRC1 == nilPair && sameRC2 == nilPair) {
                index.second = srcIndex.second / 2;
            }
            else {
                
                // global knowledge approach
                INDEX closestNextAbove = {-1,-1};
                INDEX closestNextBelow = {-1,-1};
                for (auto i : _allJoined) {
                    if (i.second == sameRC1.first) {
                        closestNextAbove = i.first;
                        break;
                    }
                }
                for (auto i : _allJoined) {
                    if (i.second == sameRC2.first) {
                        closestNextBelow = i.first;
                        break;
                    }
                }
                if (closestNextBelow.second < srcIndex.second && closestNextAbove.second < srcIndex.second
                    && closestNextBelow.second != -1 && closestNextAbove.second != -1) {
                    if ((srcIndex.second - closestNextAbove.second) < (closestNextBelow.second - srcIndex.second)) {
                        index.second = closestNextAbove.second;
                    }
                    else {
                        index.second = closestNextBelow.second;
                    }
                }
                else if (closestNextBelow.first != -1) {
                    if (closestNextBelow.second < srcIndex.second) {
                        index.second = closestNextBelow.second;
                    }
                    else {
                        index.second = srcIndex.second / 2;
                    }
                }
                else if (closestNextAbove.first != -1) {
                    if (closestNextAbove.second < srcIndex.second) {
                        index.second = closestNextAbove.second;
                    }
                    else {
                        index.second = srcIndex.second / 2;
                    }
                }
                else {
                    index.second = srcIndex.second / 2;
                }
            }
        }
    }
    else {
        index.second = srcIndex.second;

        if (srcIndex != nextOver && nextOver.first != -1)
            index.first = (srcIndex.first + nextOver.first) / 2;

        // build right case
        else if (location == "right") {

            // old solution
            //index.first = (srcIndex.first + 1) / 2;


            INDEX tmpIndex1 = {srcIndex.first, (srcIndex.second + 1) * 2};
            INDEX tmpIndex2 = {srcIndex.first, srcIndex.second / 2};
            std::pair<interfaceId,interfaceId> sameRC1 = findSameRC(tmpIndex1, 'r');
            std::pair<interfaceId,interfaceId> sameRC2 = findSameRC(tmpIndex2, 'r');
            if (sameRC1 == nilPair && sameRC2 == nilPair) {
                index.first = (srcIndex.first + 1) / 2;
            }
            else {
                // global knowledge approach
                INDEX closestNextAbove = {-1,-1};
                INDEX closestNextBelow = {-1,-1};
                for (auto i : _allJoined) {
                    if (i.second == sameRC1.second) {
                        closestNextAbove = i.first;
                        break;
                    }
                }
                for (auto i : _allJoined) {
                    if (i.second == sameRC2.second) {
                        closestNextBelow = i.first;
                        break;
                    }
                }
                if (closestNextBelow.first > srcIndex.first && closestNextAbove.first > srcIndex.first) {
                    if ((closestNextAbove.first - srcIndex.first) < (srcIndex.first - closestNextBelow.first)) {
                        index.first = closestNextAbove.first;
                    }
                    else {
                        index.first = closestNextBelow.first;
                    }
                }
                else if (closestNextBelow.first != -1) {
                    if (closestNextBelow.first > srcIndex.first) {
                        index.first = closestNextBelow.first;
                    }
                    else {
                        index.first = (srcIndex.first + 1) / 2;
                    }
                }
                else if (closestNextAbove.first != -1) {
                    if (closestNextAbove.first > srcIndex.first) {
                        index.first = closestNextAbove.first;
                    }
                    else {
                        index.first = (srcIndex.first + 1) / 2;
                    }
                }
                else {
                    index.first = (srcIndex.first + 1) / 2;
                }
            }
        }
        // build left case
        else {
            INDEX tmpIndex1 = {srcIndex.first, (srcIndex.second + 1) * 2};
            INDEX tmpIndex2 = {srcIndex.first, srcIndex.second / 2};
            std::pair<interfaceId,interfaceId> sameRC1 = findSameRC(tmpIndex1, 'r');
            std::pair<interfaceId,interfaceId> sameRC2 = findSameRC(tmpIndex2, 'r');
            if (sameRC1 == nilPair && sameRC2 == nilPair) {
                index.first = srcIndex.first / 2;
            }
            else {
                
                // global knowledge approach
                INDEX closestNextAbove = {-1,-1};
                INDEX closestNextBelow = {-1,-1};
                for (auto i : _allJoined) {
                    if (i.second == sameRC1.first) {
                        closestNextAbove = i.first;
                        break;
                    }
                }
                for (auto i : _allJoined) {
                    if (i.second == sameRC2.first) {
                        closestNextBelow = i.first;
                        break;
                    }
                }
                if (closestNextBelow.first < srcIndex.first && closestNextAbove.first < srcIndex.first
                    && closestNextBelow.first != -1 && closestNextAbove.first != -1) {
                    if ((srcIndex.first - closestNextAbove.first) < (closestNextBelow.first - srcIndex.first)) {
                        index.first = closestNextAbove.first;
                    }
                    else {
                        index.first = closestNextBelow.first;
                    }
                }
                else if (closestNextBelow.first != -1) {
                    if (closestNextBelow.first < srcIndex.first) {
                        index.first = closestNextBelow.first;
                    }
                    else {
                        index.first = srcIndex.first / 2;
                    }
                }
                else if (closestNextAbove.first != -1) {
                    if (closestNextAbove.first < srcIndex.first) {
                        index.first = closestNextAbove.first;
                    }
                    else {
                        index.first = srcIndex.first / 2;
                    }
                }
                else {
                    index.first = srcIndex.first / 2;
                }
            }
        }
    }

    std::cerr << publicId() << " created index: " << index.first << " " << index.second << "\n";

    return index;
}*/

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

/*
void TorusPeer::createChannels(json msg) {

    std::cerr << "Received channel message from peer " << msg["from"] << " to create channel in direction " << msg["location"] << std::endl;

    if (msg["location"] == "up") {
        _state->createUpChannel(msg);
    }
    else if (msg["location"] == "down") {
        _state->createDownChannel(msg);
    }
    else if (msg["location"] == "right") {
        _state->createRightChannel(msg);
    }
    else if (msg["location"] == "left") {
        _state->createLeftChannel(msg);
    }
}*/

/*
// peer joining is below and peer calling function is above
void TorusPeer::createUpChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);

    std::cerr << publicId() << " creating channel with "  << srcIndex.first << " " << srcIndex.second << "\n";

    if (!_joined) {

        _downId = msg["from"];
        _downIdIndex = srcIndex;

        if (msg["myUp"] != -1) {
            _upId = msg["myUp"];
            _upIdIndex.first = msg["myUpIndex"][0];
            _upIdIndex.second = msg["myUpIndex"][1];
            _index = createIndex("up", srcIndex, _upIdIndex);
            json newChannelMsg = buildChannelPayload("up");
            std::cerr << "channel load to " << _upId << "\n";
            unicastTo(newChannelMsg, _upId);
        }
        else {
            _upId = msg["from"];
            _downId = msg["from"];
            _upIdIndex = srcIndex;
            _downIdIndex = srcIndex;
            _index = createIndex("up", srcIndex, _upIdIndex);
        }

        rowRC(msg);
        
        std::cerr << "NOW JOINED\n";
        _joined = true;
    }
    else {
        _downId = msg["from"];
        _downIdIndex = srcIndex;
        if (_upId == -1) {
            _upId = _downId;
            _upIdIndex = _downIdIndex;
        }
    }
}

// peer joining is above and peer calling function is below
void TorusPeer::createDownChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);

    std::cerr << publicId() << " creating channel with "  << srcIndex.first << " " << srcIndex.second << "\n";

    if (!_joined) {

        _upId = msg["from"];
        _upIdIndex = srcIndex;

        if (msg["myDown"] != -1) {
            _downId = msg["myDown"];
            _downIdIndex.first = msg["myDownIndex"][0];
            _downIdIndex.second = msg["myDownIndex"][1];
            _index = createIndex("down", srcIndex, _downIdIndex);
            json newChannelMsg = buildChannelPayload("down");
            std::cerr << "channel load to " << _downId << "\n";
            unicastTo(newChannelMsg,_downId);
        }
        else {
            _downId = msg["from"];
            _upId = msg["from"];
            _downIdIndex = srcIndex;
            _upIdIndex = srcIndex;
            _index = createIndex("down", srcIndex, _downIdIndex);
        }

        rowRC(msg);

        std::cerr << "NOW JOINED\n";
        _joined = true;
    }
    else {
        _upId = msg["from"];
        _upIdIndex = srcIndex;
        if (_downId == -1) {
            _downId = _upId;
            _downIdIndex = _upIdIndex;
        }
    }
}

void TorusPeer::createRightChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);

    std::cerr << publicId() << " creating channel with "  << srcIndex.first << " " << srcIndex.second << "\n";

    if (!_joined) {

        _leftId = msg["from"];
        _leftIdIndex = srcIndex;

        if (msg["myRight"] != -1) {
            _rightId = msg["myRight"];
            _rightIdIndex.first = msg["myRightIndex"][0];
            _rightIdIndex.second = msg["myRightIndex"][1];
            _index = createIndex("right", srcIndex, _rightIdIndex);
            json newChannelMsg = buildChannelPayload("right");
            std::cerr << "channel load to " << _rightId << "\n";
            unicastTo(newChannelMsg, _rightId);
        }
        else {
            _leftId = msg["from"];
            _rightId = msg["from"];
            _leftIdIndex = srcIndex;
            _rightIdIndex = srcIndex;
            _index = createIndex("right", srcIndex, _rightIdIndex);
        }

        columnRC(msg);

        std::cerr << "NOW JOINED\n";
        _joined = true;
    }
    else {
        _leftId = msg["from"];
        _leftIdIndex = srcIndex;
        if (_rightId == -1) {
            _rightId = _leftId;
            _rightIdIndex = _leftIdIndex;
        }
    }
}

void TorusPeer::createLeftChannel(json msg) {

    INDEX srcIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);

    std::cerr << publicId() << " creating channel with "  << srcIndex.first << " " << srcIndex.second << "\n";


    if (!_joined) {

        _rightId = msg["from"];
        _rightIdIndex = srcIndex;

        if (msg["myLeft"] != -1) {
            _leftId = msg["myLeft"];
            _leftIdIndex.first = msg["myLeftIndex"][0];
            _leftIdIndex.second = msg["myLeftIndex"][1];
            _index = createIndex("left", srcIndex, _leftIdIndex);
            json newChannelMsg = buildChannelPayload("left");
            std::cerr << "channel load to " << _leftId << "\n";
            unicastTo(newChannelMsg, _leftId);
        }
        else {
            _leftId = msg["from"];
            _rightId = msg["from"];
            _leftIdIndex = srcIndex;
            _rightIdIndex = srcIndex;
            _index = createIndex("left", srcIndex, _leftIdIndex);
        }

        columnRC(msg);

        std::cerr << "NOW JOINED\n";
        _joined = true;
    }
    else {
        _rightId = msg["from"];
        _rightIdIndex = srcIndex;
        if (_leftId == -1) {
            _leftId = _rightId;
            _leftIdIndex = _rightIdIndex;
        }
    }
}*/

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

/*
void TorusPeer::computationNotJoined(json msg) {

    if (msg["type"] == "route") {
        std::cerr << publicId() << " received route with next: " << msg["nextPeer"] << "\n";
        if (_dest.first == -1)  {
            if ((msg["funds"] > _funds && _lastMessage["funds"] < _funds) ||
                (msg["funds"] < _funds && _lastMessage["funds"] > _funds)) {
                // sets dest to node that peer wants to join
                _dest = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
                json newJoinMsg = buildJoinPayload(_dest);
                std::cerr << publicId() << " is sending join message to " << msg["from"] << " with dest: " << _dest.first << " " << _dest.second << "\n";
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

}*/

/*
void TorusPeer::computationJoined(json msg) {

    if (msg["type"] == "join") {
        if (msg["destination"][0] != -1) {

            // row first
            if (0 == randMod(2)) {
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
            } 
            else {
                if (_index.second < msg["destination"][1] && _upId != -1 && _upIdIndex.second > _index.second) {
                    json message = buildRoutePayload(_upId);
                    unicastTo(message, msg["from"]);
                }
                else if (_index.second > msg["destination"][1] && _downId != -1 && _downIdIndex.second < _index.second) {
                    json message = buildRoutePayload(_downId);
                    unicastTo(message, msg["from"]);
                }
                else if (_index.first > msg["destination"][0] && _leftId != -1 && _leftIdIndex.first < _index.first) {
                    json message = buildRoutePayload(_leftId);
                    unicastTo(message, msg["from"]);
                }
                else if (_index.first < msg["destination"][0] && _rightId != -1 && _rightIdIndex.first > _index.first) {
                    json message = buildRoutePayload(_rightId);
                    unicastTo(message, msg["from"]);
                }
            } 
            if (_index.first == msg["destination"][0] && _index.second == msg["destination"][1]) {
                if (msg["funds"] > _funds) {
                    if ((_rightId == -1 && _upId == -1 )|| (_rightId != -1 && _upId != -1)) { 

                        if (randMod(2) == 0) {
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";    
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
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                            std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                        std::cerr << "building channel payload for router " << msg["from"] << "\n";
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
                        std::cerr << "building channel payload for router " << msg["from"]  << "\n";
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
        std::cerr << "joined peer " <<  publicId() << " received channel message from peer " << msg["from"] << " to create channel in direction " << msg["location"] << std::endl;
        createChannels(msg);
        json reply = buildResponsePayload();
        unicastTo(reply, msg["from"]);
    }
    else if (msg["type"] == "response") {

        std::cerr << publicId() << " xxx received response message from peer " << msg["from"] << " with index: " << msg["myIndex"][0] << " " << msg["myIndex"][1] << std::endl;

        if (msg["from"] == _rightId) {
            _rightIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _upId) {
            _upIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _leftId) {
            _leftIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
        if (msg["from"] == _downId) {
            _downIdIndex = std::make_pair(msg["myIndex"][0], msg["myIndex"][1]);
        }
    }
}*/


void TorusPeer::performComputation() {

    Packet packet;

    /*
    if (!_joined && _readyToJoin) {
        // returns index of peer with hole that 
        // has closest funds to caller
        //std::cerr << "peer: " << publicId() << " is looking for hole with closest funds\n";
        std::pair<double,double> newDest = findBestHole();
        if (newDest != _dest)
            std::cerr << "peer: " << publicId() << " found hole with index: " << newDest.first << " " << newDest.second << "\n";

        // if hole status changed to no hole, scrap all messages unless its a create channel message
        if (newDest.first == -1 && _dest.first != -1) {
            while(!inStreamEmpty()) {
                packet = popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }
            else {
            _dest = {-1,-1};
            json message = buildJoinPayload(_dest);
            std::cerr << publicId() << " is sending bootstrap join message to " << _bootStrap << " with dest: " << _dest.first << " " << _dest.second << "\n";
            unicastTo(message, _bootStrap);
            }
        }
        // if hole status changed, scrap all messages unless its a create channel message
        else if (_dest.first == -1 && newDest.first != -1) {
            while (!inStreamEmpty()) {
                packet = popInStream();
                if (packet.getMessage()["type"] == "channel")
                    break;
            }
            if (packet.getMessage()["type"] == "channel") {
                createChannels(packet.getMessage());
            }
            else {
            _dest = newDest;
            json message = buildJoinPayload(_dest);
            std::cerr << publicId() << " is sending bootstrap join message to " << _bootStrap << " with dest: " << _dest.first << " " << _dest.second << "\n";
            unicastTo(message, _bootStrap);
            }
        }
    }
    */

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