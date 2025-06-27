/*
Copyright 2022

This file is part of QUANTAS.
QUANTAS is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
QUANTAS is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with QUANTAS. If not, see <https://www.gnu.org/licenses/>.
*/

#include <iostream>
#include "PaxosPeer.hpp"

namespace quantas {

	PaxosPeer::~PaxosPeer() {

	}

	PaxosPeer::PaxosPeer(const PaxosPeer& rhs) : Peer<PaxosPeerMessage>(rhs) {
		
	}

	PaxosPeer::PaxosPeer(long id) : Peer(id) {

	}

	// clears state by setting all values to default values
	void PaxosPeer::clearState() {
		Paper tmp;
		paperData = tmp;

		Ledger tmp2;
		ledgerData = tmp2;
	}

	void PaxosPeer::checkInStrm() {
		while(!inStreamEmpty() && paperData.status != Paper::CRASHED) {
			PaxosPeerMessage newMsg = popInStream().getMessage();
			if (newMsg.messageType == "NextBallot" && newMsg.slotNumber == ledgerData.currentSlot) {
				if (newMsg.ballotNum > ledgerData.nextBal && newMsg.ballotNum > ledgerData.lastTried) {
					if (paperData.status != Paper::IDLE) {
						paperData.status = Paper::IDLE;
						paperData.quorum.clear();
						paperData.prevVotes.clear();
						paperData.voters.clear();
						paperData.paperDecree = -1;
					}

					paperData.timer = 0;
					ledgerData.nextBal = newMsg.ballotNum;
					PaxosPeerMessage reply = lastMessage();
					sendMessage(newMsg.Id, reply);
				}
			}
			else if (newMsg.messageType == "LastMessage" && newMsg.slotNumber == ledgerData.currentSlot) {
				if (paperData.status == Paper::TRYING) {
					paperData.prevVotes.push_back(newMsg);

					// only need half because the peer will vote for itself
					// therefore we get neighbors().size()/2 + 1 as majority
					if (paperData.prevVotes.size() >= neighbors().size()/2) {
						// beginBallot function sets status to polling
						// so this will only occur once
						
						PaxosPeerMessage poll = beginBallot();
						
						// adding ids to quorum set
						for (int i = 0; i < paperData.prevVotes.size(); ++i) {
							paperData.quorum.insert(paperData.prevVotes[i].Id);
						}

						for (auto i : paperData.quorum) {
							sendMessage(i, poll);
						}
					}
				}
			}
			else if (newMsg.messageType == "BeginBallot" && newMsg.slotNumber == ledgerData.currentSlot) {
				if (newMsg.ballotNum == ledgerData.nextBal && newMsg.ballotNum > ledgerData.prevBal) {
					ledgerData.prevBal = newMsg.ballotNum;
					ledgerData.ledgerDecree = newMsg.decree;

					paperData.timer = 0;

					PaxosPeerMessage reply = voted();
					sendMessage(newMsg.Id, reply);
				}
			}
			else if (newMsg.messageType == "Voted" && newMsg.slotNumber == ledgerData.currentSlot) {
				// submitBallot function deals with checking if all members of the quorum have voted
				if (paperData.status == Paper::POLLING) {
					paperData.voters.insert(newMsg.Id);
					if (paperData.voters.size() == paperData.prevVotes.size()) {
						ledgerData.outcome = newMsg.decree;
						PaxosPeerMessage reply = success();
						broadcast(reply);

						latency += getRound() - roundSent;
						++throughput;

						confirmedTrans.insert(std::make_pair(ledgerData.currentSlot,ledgerData.outcome));

						// once consensus is achieved the peer starts working on next slot
						++ledgerData.currentSlot;
						int tmpSlot = ledgerData.currentSlot;
						clearState();
						ledgerData.currentSlot = tmpSlot;
					}
				}
			}
			else if (newMsg.messageType == "Success" ) {
				ledgerData.outcome = newMsg.decree;
				confirmedTrans.insert(std::make_pair(newMsg.slotNumber,newMsg.decree));
				++ledgerData.currentSlot;
				int tmpSlot = ledgerData.currentSlot; 
				clearState(); // sets ledger to default values so slot must be set again
				ledgerData.currentSlot = tmpSlot;
			}
		}
	}

	// initializes and returns a nextBallot message
	PaxosPeerMessage PaxosPeer::nextBallot() {
		// increments 1 past last ballot number peer has received
		while (ballotIndex <= ledgerData.nextBal.first) {
			++ballotIndex;
		}

		if (ledgerData.nextBal.first == -1)
			ballotIndex = 1;

		ledgerData.lastTried.first = ballotIndex;
		ledgerData.lastTried.second = id();

		PaxosPeerMessage message;
		message.messageType = "NextBallot";
		message.ballotNum = ledgerData.lastTried;
		message.slotNumber = ledgerData.currentSlot;
		message.Id = id();
		
		paperData.status = Paper::TRYING;
		paperData.prevVotes.clear();
		paperData.quorum.clear();
		paperData.voters.clear();

		return message;
	}

	// initializes and returns a lastMessage message
	PaxosPeerMessage PaxosPeer::lastMessage() {
		PaxosPeerMessage message;
		message.messageType = "LastMessage";
		message.lastVoted = ledgerData.prevBal;
		message.ballotNum = ledgerData.nextBal;
		message.slotNumber = ledgerData.currentSlot;
		message.decree = ledgerData.ledgerDecree;
		message.Id = id();
		return message;
	}

	// initializes and returns a beginBallot message
	PaxosPeerMessage PaxosPeer::beginBallot() {
		PaxosPeerMessage message;
		message.messageType = "BeginBallot";
		message.ballotNum = ledgerData.lastTried;
		message.slotNumber = ledgerData.currentSlot;
		message.Id = id();

		// decree selection takes decree of most recent vote
		// from prevVotes set
		PaxosPeerMessage largest = paperData.prevVotes[0];
		for (int i = 0; i < paperData.prevVotes.size(); ++i) {
			if (paperData.prevVotes[i].ballotNum > largest.ballotNum) {
				largest = paperData.prevVotes[i];
			}
		}

		if (largest.decree != "") {
			message.decree = largest.decree;
		}
		else {
			char randDecree = 'A' + randMod(26);
			message.decree.push_back(randDecree);
		}

		paperData.status = Paper::POLLING;
		paperData.voters.clear();

		return message;
	}

	PaxosPeerMessage PaxosPeer::voted() {
		PaxosPeerMessage message;
		message.messageType = "Voted";
		message.ballotNum = ledgerData.nextBal;
		message.decree = ledgerData.ledgerDecree;
		message.Id = id();
		message.slotNumber = ledgerData.currentSlot;

		ledgerData.prevBal = ledgerData.nextBal;
		
		return message;
	}

	PaxosPeerMessage PaxosPeer::success() {
		PaxosPeerMessage message;
		message.messageType = "Success";
		message.ballotNum = ledgerData.lastTried;
		message.decree = ledgerData.outcome;
		message.slotNumber = ledgerData.currentSlot;
		message.Id = id();

		return message;
	}

	void PaxosPeer::sendMessage(long peer, PaxosPeerMessage message) {
		Packet<PaxosPeerMessage> newMessage(getRound(), peer, id());
		newMessage.setMessage(message);
		pushToOutSteam(newMessage);
	}

	void PaxosPeer::submitBallot() {
		// checks that peer can submit a new ballot and that peer is waiting on ballot
		if (paperData.status == Paper::IDLE && ledgerData.nextBal.first != -1) {
			if (paperData.timer > messageWait) {
				PaxosPeerMessage ballot = nextBallot();
				broadcast(ballot);
				roundSent = getRound();
			}
			else
				++paperData.timer;
		}
		else if (paperData.status == Paper::IDLE && ledgerData.nextBal.first == -1) {
			PaxosPeerMessage ballot = nextBallot();
			broadcast(ballot);
			roundSent = getRound();
		}
	}

	void PaxosPeer::crash() {
		if (crashRate != 0) {
			if (0 == randMod(crashRate) && paperData.status != Paper::CRASHED) {
				Ledger tmp = ledgerData;
				clearState();
				// note: ledgerData assigned to tmp because peer retains ledger data during crash
				ledgerData = tmp;
				paperData.status = Paper::CRASHED;
			}
			else if (paperData.status == Paper::CRASHED && recoveryRate != 0) {
				if (0 == randMod(recoveryRate))
					paperData.status = Paper::IDLE;
			}
		}
	}

	void PaxosPeer::performComputation() {
		if (true)
			checkInStrm();
		if (true)
			submitBallot();
		if (true)
			crash(); 
	}

	void PaxosPeer::endOfRound(const vector<Peer<PaxosPeerMessage>*>& _peers) {
		const vector<PaxosPeer*> peers = reinterpret_cast<vector<PaxosPeer*> const&>(_peers);
		double lat = 0;
		double satisfied = 0;

		for (int i = 0; i < peers.size(); i++) {
			satisfied += peers[i]->throughput;
			lat += peers[i]->latency;
		}
		LogWriter::getTestLog()["latency"].push_back(lat / satisfied);
		LogWriter::getTestLog()["throughput"].push_back(satisfied);

		// implementing better delay decision logic worth doing in future
		if (lat != 0) {
			for (int i = 0; i < peers.size(); ++i) {
				peers[i]->messageWait = lat + 1;
			}
		}
	}
	
	Simulation<quantas::PaxosPeerMessage, quantas::PaxosPeer>* generateSim() {
        
        Simulation<quantas::PaxosPeerMessage, quantas::PaxosPeer>* sim = new Simulation<quantas::PaxosPeerMessage, quantas::PaxosPeer>;
        return sim;
    }
}
