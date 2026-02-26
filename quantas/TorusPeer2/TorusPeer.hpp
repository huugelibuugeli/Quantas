
#ifndef TORUSPEER_HPP
#define TORUSPEER_HPP

#include "../Common/Peer.hpp"
#include <pair>
#include <vector>
#include <cmath>

namespace quantas {

    class TorusPeer : public Peer{
    public:
        TorusPeer(NetworkInterface* networkInterface);
        TorusPeer(const TorusPeer&);
        ~TorusPeer() override;


        void initParameters(std::vector<Peer*>& peers, json parameters);
        void performComputation() override;
        void endOfRound(std::vector<Peer*>& peers) override;

        NetworkInterface* releaseNetworkInterface();

        bool hasHole();
        json buildJoinPayload(std::pair<double,double>);
        json buildRoutePayload(interfaceId);
        json buildChannelPayload();

        int msgsSent = 0;

        bool _bootStrap = false;
        bool _joined = false;
        bool _readyToJoin = false;

    private:
        double _funds;
        std::pair<double,double> _index;

        // used for avoiding infinite
        // cycle on torus during routing
        // double _lastFundSeen = 0.0;

        // neighbours
        interfaceId _upId = -1; // may change to interfaceId
        std::pair<double,double> _upIdIndex = {-1,-1};
        interfaceId _downId = -1;
        std::pair<double,double> _downIdIndex = {-1,-1};
        interfaceId _rightId = -1;
        std::pair<double,double> _rightIdIndex = {-1,-1};
        interfaceId _leftId = -1;
        std::pair<double,double> _leftIdIndex = {-1,-1};

        // bootstrap peer
        interfaceId _bootStrap = -1;
        Peer* nextToJoin = nullptr; // for bootstrap with global knowledge
        // destination for joining
        std::pair<double,double> _dest = {-1,-1};
        // last message received
        json _lastMessage;


        // global knowledge solution. to be altered
        // only used once destination is already found
        // to be replaced by message passing search algorithm
        std::vector<std::pair<double,double>, interfaceId> _allJoined;
        

        void checkInStrm();
    };

}

#endif