
#ifndef TORUSPEER_HPP
#define TORUSPEER_HPP

#include "../Common/Peer.hpp"
#include <utility>
#include <vector>
#include <list>
#include <algorithm>
#include <cmath>

namespace quantas {

    typedef std::pair<double,double> INDEX;

    class TorusPeer : public Peer{
    public:
        TorusPeer(NetworkInterface* networkInterface);
        //TorusPeer(const TorusPeer&);
        ~TorusPeer() override;


        void initParameters(const std::vector<Peer*>& peers, json parameters);
        void performComputation() override;
        void endOfRound(std::vector<Peer*>& peers) override;

        //NetworkInterface* releaseNetworkInterface();

        bool hasHole();
        std::vector<std::pair<std::pair<double,double>,double>> findHoles(std::vector<Peer*>);
        std::pair<double,double> findBestHole();
        void createChannels(json);
        std::pair<interfaceId, interfaceId> findSameRC(std::pair<double,double>, char);
        std::pair<double,double> createIndex(std::string, std::pair<double,double>,std::pair<double,double>);

        void computationJoined(json);
        void computationNotJoined(json);

        // packet factory functions
        json buildJoinPayload(std::pair<double,double>) const;
        json buildRoutePayload(interfaceId) const;
        json buildChannelPayload(std::string) const;
        json buildResponsePayload() const;

        int msgsSent = 0;

        bool _isBootStrap = false;
        bool _joined = false;
        bool _readyToJoin = false;

    private:
        double _funds;
        std::pair<double,double> _index = {-1,-1};

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
        std::vector<std::pair<std::pair<double,double>, interfaceId>> _allJoined;
        std::vector<std::pair<std::pair<double,double>, double>> _allHoles;
        
        int _startedSearch = -1;

        void checkInStrm();
    };

}

#endif