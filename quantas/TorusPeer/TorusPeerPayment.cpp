// handles payments for TorusPeer

#include "TorusPeer.hpp"

namespace quantas {

void JoinedState::startPayment() {

    std::pair<Transaction,bool> t = _peer->getTransaction();

    if (!_paymentSearchStarted && t.second && !_rebalanceSearchStarted) {

        double amount = t.first._amount;
        interfaceId dest = t.first._target;

        bool foundNext = false;

        _paymentSearchStarted = true;

        PaymentPath* root = new PaymentPath(nullptr, _peer->publicId(),false);

        if (_rightChannel != -1) {

            double otherFunds = _peer->getChannel(_rightChannel)._other;
            double myFunds = _peer->getChannel(_rightChannel)._mine;
            double channelCap = otherFunds + myFunds;
            if (myFunds >= amount) {
                _pathPtrs.push_back(new PaymentPath(root,_peer->_rightId,false));
                _paymentVisitedBFS.push_back(_peer->publicId());
                //_paymentVisitedBFS.push_back(_peer->_rightId);
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg,_peer->_rightId);
                //_paymentToVisitBFS.push_back(std::make_pair(2,_rightId));
                foundNext = true;
            }
            else if (channelCap >= amount) {
                _pathPtrs.push_back(new PaymentPath(root,_peer->_rightId,true));
                _paymentVisitedBFS.push_back(_peer->publicId());
                //_paymentVisitedBFS.push_back(_peer->_rightId);
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg,_peer->_rightId);
                //_paymentToVisitBFS.push_back(std::make_pair(2,_rightId));
                foundNext = true;
            }
            //else 
                //std::cerr << "cant use right channel" << std::endl;

        }
        else if (_rightChannel == -1) {
            //std::cerr << "RIGHT CHANNEL IS -1\n";
        }
        else {
            std::cerr << "NO CONDITION WAS TRUE\n";
        }


        if (_upChannel != -1) {
            double myFunds = _peer->getChannel(_rightChannel)._mine;


            //update such that rebalancing can also happen with 
            // immediate neighbours. add check for capacity
            if (myFunds >= amount) {
                _pathPtrs.push_back(new PaymentPath(root, _peer->_upId, false));
                _paymentVisitedBFS.push_back(_peer->publicId());
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg, _peer->_upId);
                foundNext = true;
            }
            else if (myFunds >= amount) {
                _pathPtrs.push_back(new PaymentPath(root, _peer->_upId, true));
                _paymentVisitedBFS.push_back(_peer->publicId());
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg, _peer->_upId);
                foundNext = true;
            }  
            //else
                //std::cerr << " up channel cant be used " << _peer->getChannel(_upChannel)._mine << std::endl;

        }
        else if (_upChannel == -1) {
            //std::cerr << "UP CHANNEL IS -1\n";
        }
        else {
            //std::cerr << "NO CONDITION WAS TRUE\n";
        }

        if (!foundNext) {
            //std::cerr << _peer->publicId() << " has no neighbour with enough capacity\n";
        }
        
    }
}

std::pair<std::vector<interfaceId>,bool> JoinedState::paymentReady() {
    if (_hasRoute)
        return std::make_pair(_tryingRoute,true); 
    else {
        std::vector<interfaceId> tmp;
        return std::make_pair(tmp,false);
    }
}

void JoinedState::startRebalance() {

    if (!_rebalanceSearchStarted && !_paymentSearchStarted) {

        RebalanceTx transaction = _rebalanceQueue.front();
        double rebalanceAmount = transaction._amount;
        interfaceId rebalanceWith = transaction._rebalanceWith; 


        bool foundNext = false;

        _rebalanceSearchStarted = true;

        PaymentPath* root = new PaymentPath(nullptr, _peer->publicId(),false);

        if (_leftChannel != -1) {

            double myFunds = _peer->getChannel(_leftChannel)._mine;
            if (myFunds >= rebalanceAmount) {
                _rebalancePathPtrs.push_back(new PaymentPath(root,_peer->_leftId,false));
                json msg = _peer->buildRebalanceRoutePayload(true);
                _peer->unicastTo(msg,_peer->_leftId);
                foundNext = true;
                ++_rebalanceRouteReqsSent;
            }
            //else 
                //std::cerr << "--- LEFT CHANNEL HAS ONLY " << _peer->getChannel(_leftChannel)._mine << std::endl;

        }

        if (_downChannel != -1) {

            double myFunds = _peer->getChannel(_downChannel)._mine;
            if (myFunds >= rebalanceAmount) {
                _rebalancePathPtrs.push_back(new PaymentPath(root,_peer->_downId,false));
                json msg = _peer->buildRebalanceRoutePayload(true);
                _peer->unicastTo(msg,_peer->_downId);
                foundNext = true;
                ++_rebalanceRouteReqsSent;
            }
            //else 
                //std::cerr << "--- LEFT CHANNEL HAS ONLY " << _peer->getChannel(_downChannel)._mine << std::endl;

        }

        if (rebalanceWith == _peer->_rightId) {
            double otherFunds = _peer->getChannel(_rightChannel)._other;
            if (otherFunds < rebalanceAmount) {

                // rebalance not currently possible
                foundNext = false;

            }
        }
        else if (rebalanceWith == _peer->_upId) {
            double otherFunds = _peer->getChannel(_upChannel)._other;
            if (otherFunds < rebalanceAmount) {

                // rebalance not currently possible
                foundNext = false;

            }
        }
        else {
            // something went wrong. rebalance failed
        }

        if (!foundNext) {

            //std::cerr << "REBALANCE FAILED\n";
            interfaceId requester = _rebalanceQueue.front()._requester;
            json reply = _peer->buildRebalanceResultPayload(false);
            _peer->unicastTo(reply,requester);
            _rebalanceQueue.pop_front();
            _rebalanceSearchStarted = false;
            _rebalanceRouteReqsSent = 0;

        }

    }


}


std::pair<double,double> JoinedState::getFunds(std::string location) {
    PaymentChannel ch;
    if (location == "up") {
        ch = _peer->getChannel(_upChannel);
    }
    else if (location == "down") {
        ch = _peer->getChannel(_downChannel);
    }
    else if (location == "right") {
        ch = _peer->getChannel(_rightChannel);
    }
    else if (location == "left") {
        ch = _peer->getChannel(_leftChannel);
    }
    else {
        return std::make_pair(-1.0,-1.0);
    }

    return std::make_pair(ch._mine,ch._other);
}

double JoinedState::distributeFunds(double totalFunds, double availableFunds) {
    
    // distributing up
    if (_upChannel == -1 && _peer->_upId != -1 && totalFunds/4 <= availableFunds) {
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_upId;
        availableFunds -= tmpChannel._mine;
        _upChannel = _peer->addPaymentChannel(tmpChannel);
    }
    // distributing right
    if (_rightChannel == -1 && _peer->_rightId != -1 && totalFunds/4 <= availableFunds) {
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_rightId;
        availableFunds -= tmpChannel._mine;
        _rightChannel = _peer->addPaymentChannel(tmpChannel);
    }
    // distributing down
    if (_downChannel == -1 && _peer->_downId != -1 && totalFunds/4 <= availableFunds) {
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_downId;
        availableFunds -= tmpChannel._mine;
        _downChannel = _peer->addPaymentChannel(tmpChannel);
    }
    // distributing left
    if (_leftChannel == -1 && _peer->_leftId != -1 && totalFunds/4 <= availableFunds) {
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_leftId;
        availableFunds -= tmpChannel._mine;
        _leftChannel = _peer->addPaymentChannel(tmpChannel);
    }

    return availableFunds;
}

void JoinedState::paymentRoute(json msg) {
    
    // BFS SEARCH

    std::pair<Transaction, bool> t = _peer->getTransaction();

    if (t.second) {

    double amount = t.first._amount;
    interfaceId dest = t.first._target;

    // success result
    if (dest == msg["from"]) {
        std::vector<interfaceId> route;
        //int tmpIndex = msg["parentIndex"];
        //route.push_back(msg["from"]);

        PaymentPath* tmpPtr;

        for (auto* i : _pathPtrs) {
            if (i->_peerId == msg["from"]) {
                tmpPtr = i;
                break;
            }
        }

        ++_rebalancingSession;

        int rebalancesNeeded = 0;
        while (tmpPtr->_parent != nullptr) {

            if (tmpPtr->_rebalanceNeeded) {
                json rebalanceMsg = _peer->buildRebalanceRequestPayload(amount,tmpPtr->_parent->_peerId, _rebalancingSession);
                _peer->unicastTo(rebalanceMsg,tmpPtr->_parent->_peerId);
                ++rebalancesNeeded;
            }

            route.push_back(tmpPtr->_peerId);
            tmpPtr = tmpPtr->_parent;

        }

        _rebalancesNeeded = rebalancesNeeded;

        route.push_back(tmpPtr->_peerId);

        _tryingRoute = std::move(route);

        //int routeIndex = _paymentRoutes.size()-1;

        if (rebalancesNeeded == 0) {            
            makePayment();
        }
        else {

            // WHERE TO PUT INDEX??
            // in json message
            _rebalanceRouteReqsSent = rebalancesNeeded;
            _rebalancingRoute = true;;
        }
        
        
    }

    // non-success result
    else {

        //std::cerr << "ATTEMPTING TO ADD NEXT PEER TO ROUTE\n";

        interfaceId from = msg["from"];
        double rightFunds = msg["rightCapacity"][0];
        double upFunds = msg["upCapacity"][0];

        double rightTmp = msg["rightCapacity"][1];
        double rightCapacity = rightFunds + rightTmp;

        double upTmp = msg["upCapacity"][1];
        double upCapacity = upFunds + upTmp;

        if (msg["myRight"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() && rightFunds > amount) {
            
            for (auto* i : _pathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myRight"],false));
                    _paymentToVisitBFS.push_back(msg["myRight"]);
                    break;
                }
            }
        }
        else if (msg["myRight"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() &&  rightCapacity > amount) {

            LogWriter::pushValue("usedCapacity",1);

            // same but rebalance required
            for (auto* i : _pathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myRight"],true));
                    _paymentToVisitBFS.push_back(msg["myRight"]);
                    break;
                }
            }

        }
        else {
            //LogWriter::pushValue("noFUNDS", 1);
        }




        if (msg["myUp"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() && upFunds > amount) {
            
            for (auto* i : _pathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myUp"],false));
                    _paymentToVisitBFS.push_back(msg["myUp"]);
                    break;
                }
            }            
        }
        else if (msg["myUp"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() && upCapacity > amount) {


            // same but rebalance required
            for (auto* i : _pathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myRight"],true));
                    _paymentToVisitBFS.push_back(msg["myRight"]);
                    break;
                }
            }

        }

        _paymentVisitedBFS.push_back(from);
    }

    double paymentAmount = amount;

    // wont send new paymentRoute messages 
    while(!_paymentToVisitBFS.empty() && !_rebalancingRoute) {
        interfaceId current = _paymentToVisitBFS.front();
        _paymentToVisitBFS.pop_front();

        json message = _peer->buildPaymentRoutePayload(paymentAmount,true);
        _peer->unicastTo(message, current);
    }

    } // end of t.second

}

void JoinedState::makePayment() {
    //_hasRoute = goodRouteIndex;
    _hasRoute = true;

}


// works for both clearing rebalance and payment txs
void JoinedState::paymentReset() {
    _rebalancingRoute = false;
    _paymentSearchStarted = false; 
    _rebalanceHasRoute = false;
    _hasRoute = false; 
    //_paymentRoutes.clear();
    _tryingRoute.clear();
    _rebalanceRoute.clear();

    // memory cleanup
    for (auto* i : _pathPtrs) {
        delete i;
    }
    _pathPtrs.clear();

    _paymentToVisitBFS.clear(); 
    _paymentVisitedBFS.clear();
    _rebalanceToVisitBFS.clear();
    _rebalanceVisitedBFS.clear();
    _rebalanceRouteReqsSent = 0;
    _rebalancePathPtrs.clear();
}

void JoinedState::rebalanceRoute(json msg) {

    double amount = _rebalanceQueue.front()._amount;

    // two success cases. Right or up peer found during search


    if (msg["from"] == _peer->publicId()) {

        std::vector<interfaceId> route;
        //int tmpIndex = msg["parentIndex"];
        //route.push_back(msg["from"]);


        PaymentPath* tmpPtr;

        for (auto* i : _pathPtrs) {
            if (i->_peerId == msg["from"]) {
                tmpPtr = i;
                break;
            }
        }

        while (tmpPtr->_parent != nullptr) {
            route.push_back(tmpPtr->_peerId);
            tmpPtr = tmpPtr->_parent;
        }

        _tryingRoute = std::move(route);
        _hasRoute = true;


    }


    else {

        auto itr = std::find(_rebalanceVisitedBFS.begin(), _rebalanceVisitedBFS.end(), msg["from"]);

        if (msg["myLeft"] != -1 && itr == _rebalanceVisitedBFS.end() && msg["leftCapacity"] > amount) {

            for (auto* i : _rebalancePathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _rebalancePathPtrs.push_back(new PaymentPath(i,msg["myLeft"],false));
                    _rebalanceToVisitBFS.push_back(msg["myLeft"]);
                    break;
                }

            }          

        }

        if (msg["myDown"] != -1  && itr == _rebalanceVisitedBFS.end() && msg["downCapacity"] > amount) {
        
           for (auto* i : _rebalancePathPtrs) {

                if (i->_peerId == msg["from"]) {
                    _rebalancePathPtrs.push_back(new PaymentPath(i,msg["myDown"],false));
                    _rebalanceToVisitBFS.push_back(msg["myDown"]);
                    break;
                }

            }          
        }
    }

    while (!_rebalanceToVisitBFS.empty()) {

        interfaceId current = _rebalanceToVisitBFS.front();
        _rebalanceToVisitBFS.pop_front();

        json message = _peer->buildPaymentRoutePayload(amount,true);
        _peer->unicastTo(message, current);

    }

    

};

} // end of quantas namespace