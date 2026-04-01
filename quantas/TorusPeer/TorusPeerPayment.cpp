// handles payments for TorusPeer

#include "TorusPeer.hpp"


void JoinedState::paymentRoute(interfaceId destination, interfaceId nextHop, double amount) {
    
    // BFS SEARCH

    // only right and up neighbours are added
    // because non-rebalancing payments are always routed in a positive
    // direction in the torus (i.e. right and up)
    if (!paymentSearchStarted) {
        paymentSearchStarted = true;
        _paymentVisitedBFS.push_back(publicId());
        _paymentToVisitBFS.push_back(_rightId);
        _paymentToVisitBFS.push_back(_upId);
        _paymentRouteTree.push_back();
    }


    while(!_paymentToVisitBFS.empty()) {
        interfaceId current = _paymentToVisitBFS.front();
        _paymentToVisitBFS.pop_front();
        _paymentVisitedBFS.push_back(current);




    
        // if destination is found, make payment

    }

    if (destination == nextHop) {
        makePayment(route, amount);
    }
    else {
        // find next hop

}

void JoinedState::makePayment(std::vector<interfaceId> route, double amount) {
    // find channel to destination
    


}