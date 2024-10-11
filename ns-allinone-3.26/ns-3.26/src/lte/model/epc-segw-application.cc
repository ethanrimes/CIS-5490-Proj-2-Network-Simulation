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
 * Author: Sleepyboy Huang po-han.huang@nokia.com
 */

#include "epc-segw-application.h"
#include "ns3/log.h"
#include "ns3/mac48-address.h"
#include "ns3/ipv4.h"
#include "ns3/inet-socket-address.h"
#include "ns3/uinteger.h"
#include "ns3/abort.h"
#include "epc-gtpu-header.h"
#include "eps-bearer-tag.h"
#include <iostream>

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("EpcSegwApplication");

TypeId
EpcSegwApplication::GetTypeId (void)
{
  static TypeId tid = TypeId ("ns3::EpcSegwApplication")
    .SetParent<Object> ()
    .SetGroupName("Lte");
  return tid;
}

void
EpcSegwApplication::DoDispose (void)
{
  NS_LOG_FUNCTION (this);
  m_xwSocket->SetRecvCallback (MakeNullCallback<void, Ptr<Socket> > ());
  m_xwSocket = 0;
}


EpcSegwApplication::EpcSegwApplication (const Ptr<VirtualNetDevice> tunDevice, const Ptr<Socket> xwSocket)
  : m_xwSocket (xwSocket),
    m_tunDevice (tunDevice)
{
  NS_LOG_FUNCTION (this << tunDevice << xwSocket);
  m_xwSocket->SetRecvCallback (MakeCallback (&EpcSegwApplication::RecvFromXwSocket, this));
}


EpcSegwApplication::~EpcSegwApplication (void)
{
  NS_LOG_FUNCTION (this);
}
// Receive packet from outside world
bool 
EpcSegwApplication::RecvFromTunDevice (Ptr<Packet> packet, const Address& source, const Address& dest, uint16_t protocolNumber) 
{  
  // get IP address of eNB
  Ptr<Packet> pCopy = packet->Copy ();
  Ipv4Header ipv4Header;
  pCopy->RemoveHeader (ipv4Header);
  Ipv4Address eNBAddr =  ipv4Header.GetDestination ();
  NS_LOG_LOGIC ("packet addressed to eNB " << eNBAddr);
  SendToXwSocket (packet, m_enbXwAddress);
  const bool succeeded = true;
  return succeeded;
}

void 
EpcSegwApplication::RecvFromXwSocket (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);  
  NS_ASSERT (socket == m_xwSocket);
  Ptr<Packet> packet = socket->Recv ();
  SendToTunDevice (packet);
}

void 
EpcSegwApplication::SendToTunDevice (Ptr<Packet> packet)
{
  NS_LOG_LOGIC (" packet size: " << packet->GetSize () << " bytes");
  m_tunDevice->Receive (packet, 0x0800, m_tunDevice->GetAddress (), m_tunDevice->GetAddress (), NetDevice::PACKET_HOST);
}
// Send packets to eNB
void 
EpcSegwApplication::SendToXwSocket (Ptr<Packet> packet, Ipv4Address enbAddr)
{
  //NS_LOG_FUNCTION (this << packet << enbAddr << teid);
  uint32_t flags = 0;
  m_xwSocket->SendTo (packet, flags, InetSocketAddress (enbAddr, 500));
}

void 
EpcSegwApplication::AddEnb (Ipv4Address enbAddr, Ipv4Address segwAddr)
{
  //NS_LOG_FUNCTION (this << cellId << enbAddr << segwAddr);
  m_enbXwAddress = enbAddr;
  m_segwXwAddress = segwAddr;
}


}  // namespace ns3
