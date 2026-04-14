// handles payments for TorusPeer

#include "TorusPeer.hpp"

namespace quantas {

void JoinedState::startPayment() {

    std::pair<Transaction,bool> t = _peer->getTransaction();

    if (!_paymentSearchStarted && t.second) {

        double amount = t.first._amount;
        interfaceId dest = t.first._target;

        std::cerr <<_peer->publicId() <<  " STARTING FIRST PAYMENT WITH AMOUNT " << amount << std::endl;

        bool foundNext = false;

        _paymentSearchStarted = true;

        PaymentPath* root = new PaymentPath(nullptr, _peer->publicId());

        if (_rightChannel != -1) {

            double myFunds = _peer->getChannel(_rightChannel)._mine;
            if (myFunds >= amount) {
                _pathPtrs.push_back(new PaymentPath(root,_peer->_rightId));
                _paymentVisitedBFS.push_back(_peer->publicId());
                //_paymentVisitedBFS.push_back(_peer->_rightId);
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg,_peer->_rightId);
                //_paymentToVisitBFS.push_back(std::make_pair(2,_rightId));
                foundNext = true;
            }
            else 
                std::cerr << "--- RIGHT CHANNEL HAS ONLY " << _peer->getChannel(_rightChannel)._mine << std::endl;

        }
        else if (_rightChannel == -1) {
            std::cerr << "RIGHT CHANNEL IS -1\n";
        }
        else {
            std::cerr << "NO CONDITION WAS TRUE\n";
        }


        if (_upChannel != -1) {
            double myFunds = _peer->getChannel(_rightChannel)._mine;

            if (myFunds >= amount) {
                _pathPtrs.push_back(new PaymentPath(root, _peer->_upId));
                _paymentVisitedBFS.push_back(_peer->publicId());
                //_paymentVBFS.push_back(_peer->_upId);
                json msg = _peer->buildPaymentRoutePayload(amount,true);
                _peer->unicastTo(msg, _peer->_upId);
                std::cerr << "SENDING PAAYMENT MESSAGE\n";
                //_paymentToVisitBFS.push_back(std::make_pair(3,_upId));
                foundNext = true;
            }  
            else
                std::cerr << "--- up CHANNEL HAS ONLY " << _peer->getChannel(_upChannel)._mine << std::endl;

        }
        else if (_upChannel == -1) {
            std::cerr << "UP CHANNEL IS -1\n";
        }
        else {
            std::cerr << "NO CONDITION WAS TRUE\n";
        }

        if (!foundNext) {
            std::cerr << _peer->publicId() << " has no neighbour with enough capacity\n";
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

    std::cerr << "INITIALIZING CHANNELS AND DISTRIBUTING\n";
    std::cerr << "WITH NEIGHBOURS " << _peer->_upId << " " << _peer->_rightId << " " << _peer->_downId << " " << _peer->_leftId << std::endl;
    
    if (_upChannel == -1 && _peer->_upId != -1 && totalFunds/4 <= availableFunds) {
        std::cerr << "DISTRIBUTING TO up\n";
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_upId;
        availableFunds -= tmpChannel._mine;
        _upChannel = _peer->addPaymentChannel(tmpChannel);
    }
    if (_rightChannel == -1 && _peer->_rightId != -1 && totalFunds/4 <= availableFunds) {
        std::cerr << "DISTRIBUTING TO right\n";
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_rightId;
        availableFunds -= tmpChannel._mine;
        _rightChannel = _peer->addPaymentChannel(tmpChannel);
    }
    if (_downChannel == -1 && _peer->_downId != -1 && totalFunds/4 <= availableFunds) {
        std::cerr << "DISTRIBUTING TO down\n";
        PaymentChannel tmpChannel;
        tmpChannel._mine = totalFunds/4;
        tmpChannel._otherId = _peer->_downId;
        availableFunds -= tmpChannel._mine;
        _downChannel = _peer->addPaymentChannel(tmpChannel);
    }
    if (_leftChannel == -1 && _peer->_leftId != -1 && totalFunds/4 <= availableFunds) {
        std::cerr << "DISTRIBUTING TO left\n";
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

    //std::cerr << "CALLING PAYMENT ROUTE\n";

    std::pair<Transaction, bool> t = _peer->getTransaction();

    if (t.second) {

    double amount = t.first._amount;
    interfaceId dest = t.first._target;

    if (dest == msg["from"]) {
        std::cerr << "FOUND DESTINATION\n";
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

        route.push_back(tmpPtr->_peerId);

        makePayment(route);
    }


    else {

        //std::cerr << "ATTEMPTING TO ADD NEXT PEER TO ROUTE\n";

        interfaceId from = msg["from"];

        if (msg["myRight"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() && msg["rightCapacity"] > amount) {
            
            for (auto* i : _pathPtrs) {
                //std::cerr << "IDS " <<  i->_peerId << std::endl;
                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myRight"]));
                    _paymentToVisitBFS.push_back(msg["myRight"]);
                    break;
                }
            }
        }
        if (msg["myUp"] != -1 && std::find(_paymentVisitedBFS.begin(),_paymentVisitedBFS.end(),msg["from"]) == _paymentVisitedBFS.end() && msg["upCapacity"] > amount) {
            
            for (auto* i : _pathPtrs) {
                //std::cerr << "IDS " <<  i->_peerId << std::endl;
                if (i->_peerId == msg["from"]) {
                    _pathPtrs.push_back(new PaymentPath(i,msg["myUp"]));
                    _paymentToVisitBFS.push_back(msg["myUp"]);
                    break;
                }
            }            
        }

        _paymentVisitedBFS.push_back(from);
    }

    double paymentAmount = amount;

    while(!_paymentToVisitBFS.empty()) {
        interfaceId current = _paymentToVisitBFS.front();
        _paymentToVisitBFS.pop_front();

        json message = _peer->buildPaymentRoutePayload(paymentAmount,true);
        _peer->unicastTo(message, current);
    }

    }

}

void JoinedState::makePayment(std::vector<interfaceId> route) {
    _hasRoute = true;
    _paymentRoute = route;
}

} // end of quantas namespace