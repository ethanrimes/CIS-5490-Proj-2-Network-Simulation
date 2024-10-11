/* -*-  Mode: C++; c-file-style: "gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2011 Centre Tecnologic de Telecomunicacions de Catalunya (CTTC)
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Author: The author is not important
 */

#ifndef EPC_SEGW_APPLICATION_H
#define EPC_SEGW_APPLICATION_H

#include <ns3/address.h>
#include <ns3/socket.h>
#include <ns3/virtual-net-device.h>
#include <ns3/traced-callback.h>
#include <ns3/callback.h>
#include <ns3/ptr.h>
#include <ns3/object.h>
#include <ns3/lte-common.h>
#include <ns3/application.h>
#include <ns3/eps-bearer.h>
#include <ns3/epc-enb-s1-sap.h>
#include <ns3/epc-s1ap-sap.h>
#include "ns3/boolean.h"
#include <map>

namespace ns3 {

/**
 * \ingroup lte
 *
 * This application is installed inside eNBs and provides the bridge functionality for user data plane packets between the radio interface and the S1-U interface.
 */
class EpcSegwApplication : public Application
{

  // inherited from Object
public:
  static TypeId GetTypeId (void);
protected:
  void DoDispose (void);

public:
  
  /** 
   * Constructor
   * 
   * \param lteSocket the socket to be used to send/receive packets to/from the LTE radio interface
   * \param s1uSocket the socket to be used to send/receive packets
   * to/from the S1-U interface connected with the SGW 
   * \param enbS1uAddress the IPv4 address of the S1-U interface of this eNB
   * \param sgwS1uAddress the IPv4 address at which this eNB will be able to reach its SGW for S1-U communications
   * \param cellId the identifier of the enb
   */
  EpcSegwApplication (const Ptr<VirtualNetDevice> tunDevice, const Ptr<Socket> xwSocket);

  /**
   * Destructor
   * 
   */
  virtual ~EpcSegwApplication (void);

 
  /** 
   * Method to be assigned to the recv callback of the LTE socket. It is called when the eNB receives a data packet from the radio interface that is to be forwarded to the SGW.
   * 
   * \param socket pointer to the LTE socket
   */
  bool RecvFromTunDevice (Ptr<Packet> packet, const Address& source, const Address& dest, uint16_t protocolNumber);

  /** 
   * Method to be assigned to the recv callback of the S1-U socket. It is called when the eNB receives a data packet from the SGW that is to be forwarded to the UE.
   * 
   * \param socket pointer to the S1-U socket
   */
  void RecvFromXwSocket (Ptr<Socket> socket);
  void AddEnb (Ipv4Address enbAddr, Ipv4Address segwAddr);

private:

  /**
   * Send a packet to the UE via the LTE radio interface of the eNB
   * 
   * \param packet t
   * \param bid the EPS Bearer IDentifier
   */
  void SendToTunDevice (Ptr<Packet> packet);

  /** 
   * Send a packet to the SGW via the S1-U interface
   * 
   * \param packet packet to be sent
   * \param teid the Tunnel Enpoint IDentifier
   */

  void SendToXwSocket (Ptr<Packet> packet, Ipv4Address enbAddr);

  /**
   * raw packet socket to send and receive the packets to and from the LTE radio interface
   */
  Ptr<Socket> m_xwSocket;
  Ptr<VirtualNetDevice> m_tunDevice;
  Ipv4Address m_segwXwAddress;

  /**
   * address of the SGW which terminates all S1-U tunnels
   */
  Ipv4Address m_enbXwAddress;
  bool m_enableSegwSPlitting;
};

} //namespace ns3

#endif /* EPC_ENB_APPLICATION_H */

