
#ifndef TORUSPEER_HPP
#define TORUSPEER_HPP

#include "../Common/Peer.hpp"
#include <utility>
#include <vector>
#include <deque>
#include <list>
#include <algorithm>
#include <cmath>
#include <memory>

namespace quantas {

    class TorusPeerState;

    typedef std::pair<int,int> INDEX;

    struct PaymentChannel {
        double _mine;
        double _other;
        interfaceId _otherId;
    };

    struct Transaction {
        double _amount;
        interfaceId _source;
        interfaceId _target;
    };


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
        bool hasHoleLocation(std::string location);
        std::vector<std::pair<INDEX,double>> findHoles(std::vector<Peer*>);
        std::pair<INDEX,bool> findBestHole();
        std::pair<interfaceId, interfaceId> findSameRC(INDEX, char);
        //INDEX createIndex(std::string, INDEX, INDEX);
        INDEX createIndex(std::string, INDEX);

        void pathFind(json msg);


        //void computationJoined(json);
        //void computationNotJoined(json);

        // changes to join state
        void changeState();

        void rowRC(json);
        void columnRC(json);

        // packet factory functions
        json buildJoinPayload(INDEX) const;
        json buildRoutePayload(interfaceId) const;
        json buildChannelPayload(std::string) const;
        json buildResponsePayload() const;
        json buildPathFindPayload(INDEX) const;
        json buildPathFindResponsePayload() const;


        // neighbours
        interfaceId _upId = -1;
        INDEX _upIdIndex;    bool _upIndexHasValue = false;
        interfaceId _downId = -1;
        INDEX _downIdIndex;  bool _downIndexHasValue = false; 
        interfaceId _rightId = -1;
        INDEX _rightIdIndex; bool _rightIndexHasValue = false;
        interfaceId _leftId = -1;
        INDEX _leftIdIndex;  bool _leftIndexHasValue = false;

        // destination for joining
        INDEX _dest;         bool _destHasValue = false;

        INDEX _index;        bool _indexHasValue = false;
        double _funds;

        // last message received
        json _lastMessage;


        bool _isBootStrap = false;
        bool _readyToJoin = false;
        bool _bootStrapSent = false;

        // bootstrap peer
        interfaceId _bootStrap = -1;
        Peer* nextToJoin = nullptr; // for bootstrap with global knowledge

        std::list<std::pair<interfaceId, INDEX>> _toVisit;
        std::set<std::pair<interfaceId, INDEX>> _visited;
        void clearToVisit() {_toVisit.clear();}
        void clearVisited() {_visited.clear();}

        bool _startedSearch = false;

        int _searchStartRound;

        // if turned true, should turn back to false after timeout
        // timeout not yet implemented. For BFS search algorithm
        bool _joinSent = false;

        // trackers for eventual index creation
        // negative indicates step towards left/down
        // positive indicates step towards right/up
        // measured from center _bootStrap node
        int _horizontalSteps = 0;
        int _verticalSteps = 0;
    private:

        std::vector<Channel> _channels;


        std::unique_ptr<TorusPeerState> _state = nullptr;

        // global knowledge solution. to be altered
        // only used once destination is already found
        // to be replaced by message passing search algorithm
        std::vector<std::pair<INDEX, interfaceId>> _allJoined;
        std::vector<std::pair<INDEX, double>> _allHoles;

        void checkInStrm();
    };


    class TorusPeerState {
    public:
        TorusPeerState(TorusPeer* peer) : _peer(peer) {}
        ~TorusPeerState() = default;

        virtual void computation(json);
        virtual void preComputation() {};

        void createChannels(json);

        // channel creation functions
        virtual void createUpChannel(json);
        virtual void createDownChannel(json);
        virtual void createRightChannel(json);
        virtual void createLeftChannel(json);

        // return joined/not joined status
        virtual bool isJoined() const = 0;

    protected:
        TorusPeer* _peer;
    };

    class JoinedState : public TorusPeerState {
    public:
        JoinedState(TorusPeer* peer) : TorusPeerState(peer) {}
        ~JoinedState() = default;

        // round computation function
        void computation(json msg) override;

        // channel creation functions
        void createUpChannel(json) override;
        void createRightChannel(json) override;
        void createDownChannel(json) override;
        void createLeftChannel(json) override;

        bool isJoined() const override {
            return true;
        }

        void makePayment(double, interfaceId);

    private:

        std::list<Transaction> _pendingTransactions;
        bool paymentSearchStarted = false;
        
        std::vector<interfaceId> _paymentVisitedBFS;
        std::vector<interfaceId> _paymentToVisitBFS;
        std::vector<interfaceId> _paymentRouteTree;

    };

    class NotJoinedState : public TorusPeerState {
    public:
        NotJoinedState(TorusPeer* peer) : TorusPeerState(peer) {}
        ~NotJoinedState() = default;

        // round computation function
        void computation(json msg) override;
        void preComputation() override;

        // when peer first starts to join
        void findDestination();

        // channel creation functions
        void createUpChannel(json) override;
        void createRightChannel(json) override;
        void createDownChannel(json) override;
        void createLeftChannel(json) override;

        bool isJoined() const override {
            return false;
        }

    private:
        bool _createdChannel = false;
        bool _startedRoute = false;
    };

}

#endif