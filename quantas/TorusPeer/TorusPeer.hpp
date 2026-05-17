
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
#include <iterator>
#include <fstream>
#include <map>

namespace quantas {

    class TorusPeerState;

    typedef std::pair<int,int> INDEX;

    struct PaymentChannel {

        bool operator==(interfaceId id) {
            if (_otherId == id)
                return true;
            else 
                return false;
        }

        double _mine = -1;
        double _other = -1;
        interfaceId _otherId = -1;
    };

    struct Transaction {
        double _amount = -1;
        interfaceId _source = -1;
        interfaceId _target = -1;
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

        // changes to join state
        void changeState();

        // for finding peers in same row or column
        void rowRC(json);
        void columnRC(json);

        // packet factory functions
        json buildJoinPayload(INDEX) const;
        json buildRoutePayload(interfaceId) const;
        json buildChannelPayload(std::string) const;
        json buildResponsePayload() const;
        json buildPathFindPayload(INDEX) const;
        json buildPathFindResponsePayload() const;
        json buildPaymentRoutePayload(double, bool) const;
        json buildRebalanceRoutePayload(bool) const;
        json buildRebalanceRequestPayload(double, interfaceId, int) const;
        json buildRebalanceResultPayload(bool) const;

        // payment channel functions
        PaymentChannel getChannel(int location) {return _channels[location];};
        int addPaymentChannel(PaymentChannel);
        void updateChannel(int location, double myFunds, double otherFunds);
        void initChannels();
        bool tryPayment(std::vector<TorusPeer*>,Transaction);
        void updateFunds(interfaceId, double, bool);
        std::pair<Transaction,bool> getTransaction() const {if (_pendingTransactions.empty()) return std::make_pair(_pendingTransactions.front(),false); else return std::make_pair(_pendingTransactions.front(),true);}
        // returns true if has channel with given peers 
        // and enough funds to make the payment. Else false
        bool hasCapacity(interfaceId, double);
        void transactionFinished();
        // used to inform peer of how much funds other peer has in theiri channel at initialization
        void fundInitHelper(std::vector<TorusPeer*>);


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
        double _fundsAvailable;

        // last message received
        json _lastMessage;


        bool _isBootStrap = false;
        bool _readyToJoin = false;
        bool _bootStrapSent = false;

        // bootstrap peer
        interfaceId _bootStrap = -1;
        Peer* nextToJoin = nullptr; // for bootstrap with global knowledge

        // used for peer join search
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

        // indicates whether in join or payment simulation
        // eventually to be combined
        bool _preBuilt = false;

        std::vector<PaymentChannel> _channels;
        std::list<Transaction> _pendingTransactions;
        
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

        virtual void computation(json) = 0;
        virtual void preComputation() = 0;

        void createChannels(json);

        // channel creation functions
        virtual void createUpChannel(json) = 0;
        virtual void createDownChannel(json) = 0;
        virtual void createRightChannel(json) = 0;
        virtual void createLeftChannel(json) = 0;

        // return joined/not joined status
        virtual bool isJoined() const = 0;

        // distributes funds across channels
        virtual double distributeFunds(double, double) {std::cerr << "CALLING distributeFunds() IN NON-JOINED STATE\n"; exit(1);};
        virtual std::pair<std::vector<interfaceId>,bool> paymentReady() {std::vector<long> tmp; return std::make_pair(tmp,false);};
        virtual void paymentReset() {}
        virtual std::pair<double,double> getFunds(std::string) { return std::make_pair(-1,-1);};


    protected:
        TorusPeer* _peer;
    };

    class JoinedState : public TorusPeerState {
    public:
        JoinedState(TorusPeer* peer) : TorusPeerState(peer) {}
        ~JoinedState() = default;

        // round computation function
        void computation(json msg) override;
        void preComputation() override;

        // channel creation functions
        void createUpChannel(json) override;
        void createRightChannel(json) override;
        void createDownChannel(json) override;
        void createLeftChannel(json) override;

        bool isJoined() const override {
            return true;
        }

        void startPayment();
        void startRebalance();

        void paymentRoute(json msg);
        void rebalanceRoute(json msg);

        // first value represents calling peer's channel funds, second value represents neighbour's channel funds
        // string represents location of channel (i.e. up, down, right, left)
        std::pair<double,double> getFunds(std::string) override;
        double distributeFunds(double, double);


        void makePayment();
        std::pair<std::vector<interfaceId>,bool> paymentReady() override;
        // currently resetting paymentRouteTree but maybe should keep in between routes, so peer has some routes memorized and doesn't need to search again
        void paymentReset() override;
        void tryRebalance();


    private:

        struct PaymentPath {
            PaymentPath(PaymentPath* ptr, interfaceId id, bool rebalance) : _parent(ptr), _peerId(id), _rebalanceNeeded(rebalance) {}
            PaymentPath* _parent;
            interfaceId _peerId;
            // bool is whether rebalance required
            bool _rebalanceNeeded;
        };
        struct RebalanceTx {
            RebalanceTx(interfaceId r, interfaceId w, double a) : _requester(r), _rebalanceWith(w), _amount(a) {}
            interfaceId _requester;

            interfaceId _rebalanceWith; // which neighbour peer. (up or right)
            double _amount;
        };

        // index of where channel struct is stored in _channels vector represents location of channel
        int _upChannel = -1;
        int _downChannel = -1;
        int _rightChannel = -1;
        int _leftChannel = -1;

        bool _readyForPayment = false;

        // for payment BFS search
        bool _paymentSearchStarted = false;
        std::list<interfaceId> _paymentVisitedBFS;
        std::list<interfaceId> _paymentToVisitBFS;
        // if -1 no route
        // else, has index of which route successfUl in _paymentRoutes
        // int _hasRoute = -1;
        bool _hasRoute = false; // current single path approach
        std::vector<PaymentPath*> _pathPtrs;

        // for rebalance BFS search
        bool _rebalanceSearchStarted = false;
        std::list<interfaceId> _rebalanceVisitedBFS;
        std::list<interfaceId> _rebalanceToVisitBFS;
        bool _rebalanceHasRoute = false;
        std::vector<PaymentPath*> _rebalancePathPtrs;
        

        // queue of rebalance requests. Dealt with one at a time
        // stores amount and the up or right neighbour of the peer 
        // completing the rebalancing.
        std::deque<RebalanceTx> _rebalanceQueue;

        // current route that is being attempted
        // if all have funds, pay, else try rebalance
        std::vector<interfaceId> _tryingRoute;

        // found route, but some channels still require rebalancing
        bool _rebalancingRoute = false;
        int _rebalancingSession = 0; // make sure rebalanceSuccess message is for correct session

        // number of rebalances required for each route
        // one route at a time approach
        int _rebalancesNeeded = 0;

        // final route used for rebalancing
        std::vector<interfaceId> _rebalanceRoute;

        int _rebalanceRouteReqsSent = 0;

        // if there are both payment and rebalance
        // transactions in queue, every other is a 
        // payment and every other is a rebalance
        bool _lastWasRebalance = false;

        
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