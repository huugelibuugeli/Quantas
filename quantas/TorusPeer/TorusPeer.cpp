#include"TorusPeer.hpp"

static bool registerTorusPeer = []() {
    return PeerRegistry::registerPeerType(
        "ExamplePeer",
        [](interfaceId pubId) { return new ExamplePeer(new NetworkInterfaceAbstract(pubId)); });
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

std::vector<std::pair<double,double>>& TorusPeer::findHoles(std::set<Peer*> peers) {

    std::vector<Peer*> peersWithHole;
    
    for (auto p : peers) {
        if (p->hasHole() && p->_joined)
            peersWithHole.push_back(*p);
    }

    return peersWithHole;

}

void TorusPeer::initParameters(std::vector<Peer*>& peers, json parameters) {

	const vector<TorusPeer*> peers = reinterpret_cast<vector<TorusPeer*> const&>(peers);

    peers[0]->_bootStrap = true;
    peers[0]->_joined = true;
    peers[0]->_index = {0,0};

    for (auto p : peers) {
        p->_funds = randMod(parameters["maxFunds"]);
        p->_bootStrap = peers[0]->publicId();
        std::cerr << p->_funds << " ";
    }

}

void TorusPeer::performComputation() {

    if (!_joined) {
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
            if (msg["type"] == "route") {
                if (_dest == msg["index"])
                    // buildChannelPayload()
                else if (_dest.first == -1)  {
                    if ((msg["funds"] > _funds && _lastMessage["funds"] < _funds) || 
                        (msg["funds"] < _funds && _lastMessage["funds"] > _funds)) {
                            // multicast to gap buildChannelPayload()
                    }
                    json message = buildJoinPayload({-1,-1});
                    unicastTo(message, msg["nextPeer"]);
                }
                else {
                    json message = buildJoinPayload(_dest);
                    unicastTo(message, msg["nextPeer"]);
                }
            }
        }
        else {
            if (msg["type"] == "join") {
                if (msg["destination"].first != -1) {

                    if (_index.first < msg["destination"].first) {
                        if (_rightId != -1) {
                            json message = buildRoutePayload(_rightId);
                            unicastTo(message, msg["from"]);
                        }
                    }
                    else if (_index.first > msg["destination"].first) {
                        if (_leftId != -1) {
                            json message = buildRoutePayload(_leftId);
                            unicastTo(message, msg["from"]);
                        }
                    }                    
                    else if (_index.second < msg["destination"].second) {
                        if (_upId != -1) {
                            json message = buildRoutePayload(_upId);
                            unicastTo(message, msg["from"]);
                        }
                    }
                    else if (_index.second > msg["destination"].second) {
                        if (_downId != -1) {
                            json message = buildRoutePayload(_downId);
                            unicastTo(message, msg["from"]);
                        }
                    }

                    else if (_index == msg["destination"]) {
                        if (msg["funds"] > _funds) {
                            if (_rightId == -1 && _upId == -1) { 

                                if (randMod(2) == 0) {
                                    //json message = buildChannelPayload(right);
                                    unicastTo(message, target);
                                }
                                else {
                                    //json message = buildChannelPayload(up);
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

                        }
                    }
                }
            }
        }
    }
}
        // joining as p's upId
        // my->downId = p
        // if (p->downId == nullptr)
        //  my->upId == p

json TorusPeer::buildGetHolePayload() {
    json payload;
    payload["type"];
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
    payload["index"] = _index;
    payload["nextPeer"] = nextQuery;
    
    return payload;
}

json TorusPeer::buildChannelPayload(std::string location) {
    json payload;
    payload["type"] = "channel";
    payload["from"] = publicId();
    payload["roundSent"] = RoundManager::currentRound();
    payload["location"] = location;
    
    return payload;
}

json TorusPeer::buildSettlePayload()

void TorusPeer::joinTorus() {

}