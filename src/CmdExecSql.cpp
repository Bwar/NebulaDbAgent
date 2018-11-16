/*******************************************************************************
 * Project:  NebulaDbAgent
 * @file     CmdExecSql.cpp
 * @brief 
 * @author   Bwar
 * @date:    2016年7月28日
 * @note
 * Modify history:
 ******************************************************************************/
#include "CmdExecSql.hpp"

namespace dbagent
{

CmdExecSql::CmdExecSql(int32 iCmd)
    : neb::Cmd(iCmd),
      m_iConnectionTimeout(gc_iMaxBeatTimeInterval),m_szColValue(NULL),
      m_uiSectionFrom(0), m_uiSectionTo(0), m_uiHash(0), m_uiDivisor(1)
{
    m_szColValue = new char[gc_iMaxColValueSize];
}

CmdExecSql::~CmdExecSql()
{
    if (m_szColValue != NULL)
    {
        delete[] m_szColValue;
        m_szColValue = NULL;
    }

    for (auto iter = m_mapDbiPool.begin();
        iter != m_mapDbiPool.end(); ++iter)
    {
        if (iter->second != nullptr)
        {
            delete iter->second;
            iter->second = nullptr;
        }
    }
    m_mapDbiPool.clear();

    for (auto iter = m_mapDbInstanceInfo.begin();
            iter != m_mapDbInstanceInfo.end(); ++iter)
    {
        if (iter->second != nullptr)
        {
            delete iter->second;
            iter->second = nullptr;
        }
    }
    m_mapDbInstanceInfo.clear();
}

bool CmdExecSql::Init()
{
    //配置文件路径查找
    std::string strConfFile = GetWorkPath() + "/conf/" + std::string("DbConfig.json");
    LOG4_DEBUG("CONF FILE = %s.", strConfFile.c_str());

    std::ifstream fin(strConfFile.c_str());
    if (fin.good())
    {
        std::stringstream ssContent;
        ssContent << fin.rdbuf();
        fin.close();
        if (m_oDbConf.Parse(ssContent.str()))
        {
            LOG4_TRACE("m_oDbConf pasre OK");
            char szInstanceGroup[64] = {0};
            char szFactorSectionKey[32] = {0};
            uint32 uiDataType = 0;
            uint32 uiFactor = 0;
            uint32 uiFactorSection = 0;
            if (m_oDbConf["table"].IsEmpty())
            {
                LOG4_ERROR("m_oDbConf[\"table\"] is empty!");
                return(false);
            }
            if (m_oDbConf["database"].IsEmpty())
            {
                LOG4_ERROR("m_oDbConf[\"database\"] is empty!");
                return(false);
            }
            if (m_oDbConf["cluster"].IsEmpty())
            {
                LOG4_ERROR("m_oDbConf[\"cluster\"] is empty!");
                return(false);
            }
            if (m_oDbConf["db_group"].IsEmpty())
            {
                LOG4_ERROR("m_oDbConf[\"db_group\"] is empty!");
                return(false);
            }

            for (int i = 0; i < m_oDbConf["data_type"].GetArraySize(); ++i)
            {
                if (m_oDbConf["data_type_enum"].Get(m_oDbConf["data_type"](i), uiDataType))
                {
                    for (int j = 0; j < m_oDbConf["section_factor"].GetArraySize(); ++j)
                    {
                        if (m_oDbConf["section_factor_enum"].Get(m_oDbConf["section_factor"](j), uiFactor))
                        {
                            if (m_oDbConf["factor_section"][m_oDbConf["section_factor"](j)].IsArray())
                            {
                                std::set<uint32> setFactorSection;
                                for (int k = 0; k < m_oDbConf["factor_section"][m_oDbConf["section_factor"](j)].GetArraySize(); ++k)
                                {
                                    if (m_oDbConf["factor_section"][m_oDbConf["section_factor"](j)].Get(k, uiFactorSection))
                                    {
                                        snprintf(szInstanceGroup, sizeof(szInstanceGroup), "%u:%u:%u", uiDataType, uiFactor, uiFactorSection);
                                        snprintf(szFactorSectionKey, sizeof(szFactorSectionKey), "LE_%u", uiFactorSection);
                                        setFactorSection.insert(uiFactorSection);
                                        neb::CJsonObject* pInstanceGroup
                                            = new neb::CJsonObject(m_oDbConf["cluster"][m_oDbConf["data_type"](i)][m_oDbConf["section_factor"](j)][szFactorSectionKey]);
                                        LOG4_TRACE("%s : %s", szInstanceGroup, pInstanceGroup->ToString().c_str());
                                        m_mapDbInstanceInfo.insert(std::pair<std::string, neb::CJsonObject*>(szInstanceGroup, pInstanceGroup));
                                    }
                                    else
                                    {
                                        LOG4_ERROR("m_oDbConf[\"factor_section\"][%s](%d) is not exist!",
                                                        m_oDbConf["section_factor"](j).c_str(), k);
                                        continue;
                                    }
                                }
                                snprintf(szInstanceGroup, sizeof(szInstanceGroup), "%u:%u", uiDataType, uiFactor);
                                LOG4_TRACE("%s factor size %u", szInstanceGroup, setFactorSection.size());
                                m_mapFactorSection.insert(std::pair<std::string, std::set<uint32> >(szInstanceGroup, setFactorSection));
                            }
                            else
                            {
                                LOG4_ERROR("m_oDbConf[\"factor_section\"][%s] is not a json array!",
                                                m_oDbConf["section_factor"](j).c_str());
                                continue;
                            }
                        }
                        else
                        {
                            LOG4_ERROR("missing %s in m_oDbConf[\"section_factor_enum\"]", m_oDbConf["section_factor"](j).c_str());
                            continue;
                        }
                    }
                }
                else
                {
                    LOG4_ERROR("missing %s in m_oDbConf[\"data_type_enum\"]", m_oDbConf["data_type"](i).c_str());
                    continue;
                }
            }
        }
        else
        {
            LOG4_ERROR("m_oDbConf pasre error");
            return(false);
        }
    }
    else
    {
        //配置信息流读取失败
        LOG4_ERROR("Open conf \"%s\" error!", strConfFile.c_str());
        return(false);
    }

    return(true);
}

bool CmdExecSql::AnyMessage(std::shared_ptr<neb::SocketChannel> pChannel, const MsgHead& oMsgHead, const MsgBody& oMsgBody)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    m_pChannel = pChannel;
    m_oInMsgHead = oMsgHead;
    m_oInMsgBody = oMsgBody;

    neb::Mydis oMemOperate;
    CMysqlDbi* pMasterDbi = NULL;
    CMysqlDbi* pSlaveDbi = NULL;

    if (!oMemOperate.ParseFromString(oMsgBody.data()))
    {
        LOG4_ERROR("Parse protobuf msg error!");
        Response(ERR_PARASE_PROTOBUF, "neb::Mydis ParseFromString() failed!");
        return(false);
    }

    bool bConnected = GetDbConnection(oMemOperate, &pMasterDbi, &pSlaveDbi);
    if (bConnected)
    {
        LOG4_TRACE("succeed in getting db connection");
        int iResult = 0;
        if (neb::Mydis::DbOperate::SELECT == oMemOperate.db_operate().query_type())
        {
            iResult = Query(oMemOperate, pSlaveDbi);
            if (0 == iResult)
            {
                return(true);
            }
            else
            {
                iResult = Query(oMemOperate, pMasterDbi);
                if (0 == iResult)
                {
                    return(true);
                }
                else
                {
                    Response(pMasterDbi->GetErrno(), pMasterDbi->GetError());
                    return(false);
                }
            }
        }
        else
        {
            if (NULL == pMasterDbi)
            {
                Response(pMasterDbi->GetErrno(), pMasterDbi->GetError());
                return(false);
            }
            iResult = Query(oMemOperate, pMasterDbi);
            if (0 == iResult)
            {
                return(true);
            }
            else
            {
                Response(pMasterDbi->GetErrno(), pMasterDbi->GetError());
                return(false);
            }
        }
    }
    return(false);
}

bool CmdExecSql::GetDbConnection(const neb::Mydis& oQuery,
                CMysqlDbi** ppMasterDbi, CMysqlDbi** ppSlaveDbi)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    char szFactor[32] = {0};
    int32 iDataType = 0;
    int32 iSectionFactorType = 0;
    m_oDbConf["table"][oQuery.db_operate().table_name()].Get("data_type", iDataType);
    m_oDbConf["table"][oQuery.db_operate().table_name()].Get("section_factor", iSectionFactorType);
    snprintf(szFactor, 32, "%d:%d", iDataType, iSectionFactorType);
    std::map<std::string, std::set<uint32> >::const_iterator c_factor_iter =  m_mapFactorSection.find(szFactor);
    if (c_factor_iter == m_mapFactorSection.end())
    {
        LOG4_ERROR("no db config found for data_type %d section_factor_type %d",
                        iDataType, iSectionFactorType);
        Response(ERR_LACK_CLUSTER_INFO, "no db config found for oMemOperate.cluster_info()!");
        return(false);
    }
    else
    {
        std::set<uint32>::const_iterator c_section_iter = c_factor_iter->second.lower_bound(oQuery.section_factor());
        if (c_section_iter == c_factor_iter->second.end())
        {
            LOG4_ERROR("no factor_section config found for data_type %u section_factor_type %u section_factor %u",
                            iDataType, iSectionFactorType, oQuery.section_factor());
            Response(ERR_LACK_CLUSTER_INFO, "no db config for the cluster info!");
            return(false);
        }
        else
        {
            snprintf(szFactor, 32, "%u:%u:%u", iDataType, iSectionFactorType, *c_section_iter);
            m_uiSectionTo = *c_section_iter;
            --c_section_iter;
            if (c_section_iter == c_factor_iter->second.end())
            {
                m_uiSectionFrom = 0;
            }
            else
            {
                m_uiSectionFrom = *c_section_iter + 1;
            }
            std::map<std::string, neb::CJsonObject*>::iterator conf_iter = m_mapDbInstanceInfo.find(szFactor);
            if (conf_iter == m_mapDbInstanceInfo.end())
            {
                LOG4_ERROR("no db config found for %s which consist of data_type %u section_factor_type %u section_factor %u",
                                szFactor, iDataType, iSectionFactorType, oQuery.section_factor());
                Response(ERR_LACK_CLUSTER_INFO, "no db config for the cluster info!");
                return(false);
            }
            std::string strDbName = m_oDbConf["table"][oQuery.db_operate().table_name()]("db_name");
            std::string strDbGroup = m_oDbConf["database"][strDbName]("db_group");
            std::string strInstance;
            if (!conf_iter->second->Get(strDbGroup, strInstance))
            {
                Response(ERR_LACK_CLUSTER_INFO, "no db instance config for db group!");
                LOG4_ERROR("no db instance config for db group \"%s\"!", strDbGroup.c_str());
                return(false);
            }

            if (oQuery.db_operate().query_type() > atoi(m_oDbConf["db_group"][strInstance]("query_permit").c_str()))
            {
                Response(ERR_NO_RIGHT, "no right to excute oMemOperate.db_operate().query_type()!");
                LOG4_ERROR("no right to excute oMemOperate.db_operate().query_type() %d!",
                                oQuery.db_operate().query_type());
                return(false);
            }
            std::string strMasterHost = m_oDbConf["db_group"][strInstance]("master_host");
            std::string strSlaveHost = m_oDbConf["db_group"][strInstance]("slave_host");
            std::string strPort = m_oDbConf["db_group"][strInstance]("port");
            std::string strMasterIdentify = strMasterHost + std::string(":") + strPort;
            std::string strSlaveIdentify = strSlaveHost + std::string(":") + strPort;
            bool bEstablishConnection = FetchOrEstablishConnection(
                            oQuery.db_operate().query_type(),strMasterIdentify, strSlaveIdentify,
                            m_oDbConf["db_group"][strInstance], ppMasterDbi, ppSlaveDbi);
            return(bEstablishConnection);
        }
    }
}

bool CmdExecSql::FetchOrEstablishConnection(neb::Mydis::DbOperate::E_QUERY_TYPE eQueryType,
                const std::string& strMasterIdentify, const std::string& strSlaveIdentify,
                const neb::CJsonObject& oInstanceConf, CMysqlDbi** ppMasterDbi, CMysqlDbi** ppSlaveDbi)
{
    LOG4_TRACE("%s(%s, %s, %s)", __FUNCTION__, strMasterIdentify.c_str(), strSlaveIdentify.c_str(), oInstanceConf.ToString().c_str());
    *ppMasterDbi = NULL;
    *ppSlaveDbi = NULL;
    std::map<std::string, tagConnection*>::iterator dbi_iter = m_mapDbiPool.find(strMasterIdentify);
    if (dbi_iter == m_mapDbiPool.end())
    {
        tagConnection* pConnection = new tagConnection();
        if (NULL == pConnection)
        {
            Response(ERR_NEW, "malloc space for db connection failed!");
            return(false);
        }
        CMysqlDbi* pDbi = new CMysqlDbi();
        if (NULL == pDbi)
        {
            Response(ERR_NEW, "malloc space for db connection failed!");
            delete pConnection;
            pConnection = NULL;
            return(false);
        }
        pConnection->pDbi = pDbi;
        if (0 == ConnectDb(oInstanceConf, pDbi, true))
        {
            LOG4_TRACE("succeed in connecting %s.", strMasterIdentify.c_str());
            *ppMasterDbi = pDbi;
            pConnection->iQueryPermit = atoi(oInstanceConf("query_permit").c_str());
            pConnection->iTimeout = atoi(oInstanceConf("timeout").c_str());
            pConnection->ullBeatTime = time(NULL);
            m_mapDbiPool.insert(std::pair<std::string, tagConnection*>(strMasterIdentify, pConnection));
        }
        else
        {
            if (neb::Mydis::DbOperate::SELECT != eQueryType)
            {
                Response(pDbi->GetErrno(), pDbi->GetError());
            }
            delete pConnection;
            pConnection = NULL;
        }
    }
    else
    {
        dbi_iter->second->ullBeatTime = time(NULL);
        *ppMasterDbi = dbi_iter->second->pDbi;
    }

    LOG4_TRACE("find slave %s.", strSlaveIdentify.c_str());
    dbi_iter = m_mapDbiPool.find(strSlaveIdentify);
    if (dbi_iter == m_mapDbiPool.end())
    {
        tagConnection* pConnection = new tagConnection();
        if (NULL == pConnection)
        {
            Response(ERR_NEW, "malloc space for db connection failed!");
            return(false);
        }
        CMysqlDbi* pDbi = new CMysqlDbi();
        if (NULL == pDbi)
        {
            Response(ERR_NEW, "malloc space for db connection failed!");
            delete pConnection;
            pConnection = NULL;
            return(false);
        }
        pConnection->pDbi = pDbi;
        if (0 == ConnectDb(oInstanceConf, pDbi, true))
        {
            LOG4_TRACE("succeed in connecting %s.", strSlaveIdentify.c_str());
            *ppSlaveDbi = pDbi;
            pConnection->iQueryPermit = atoi(oInstanceConf("query_permit").c_str());
            pConnection->iTimeout = atoi(oInstanceConf("timeout").c_str());
            pConnection->ullBeatTime = time(NULL);
            m_mapDbiPool.insert(std::pair<std::string, tagConnection*>(strSlaveIdentify, pConnection));
        }
        else
        {
            delete pConnection;
            pConnection = NULL;
        }
    }
    else
    {
        dbi_iter->second->ullBeatTime = time(NULL);
        *ppSlaveDbi = dbi_iter->second->pDbi;
    }

    LOG4_TRACE("pMasterDbi = 0x%x, pSlaveDbi = 0x%x.", *ppMasterDbi, *ppSlaveDbi);
    if (*ppMasterDbi || *ppSlaveDbi)
    {
        return(true);
    }
    else
    {
        return(false);
    }
}

int CmdExecSql::ConnectDb(const neb::CJsonObject& oInstanceConf, CMysqlDbi* pDbi, bool bIsMaster)
{
    LOG4_DEBUG("%s()", __FUNCTION__);
    int iResult = 0;
    char szIdentify[32] = {0};
    tagDbConfDetail stDbConfDetail;
    if (bIsMaster)
    {
        strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbHost,
                        oInstanceConf("master_host").c_str(),sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbHost));
        snprintf(szIdentify, sizeof(szIdentify), "%s:%s", oInstanceConf("master_host").c_str(), oInstanceConf("port").c_str());
    }
    else
    {
        strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbHost,
                        oInstanceConf("slave_host").c_str(),sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbHost));
        snprintf(szIdentify, sizeof(szIdentify), "%s:%s", oInstanceConf("slave_host").c_str(), oInstanceConf("port").c_str());
    }
    strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbUser,oInstanceConf("user").c_str(),sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbUser));
    strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbPwd,oInstanceConf("password").c_str(),sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbPwd));
    strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbName, "test",sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbName));
    strncpy(stDbConfDetail.m_stDbConnInfo.m_szDbCharSet,oInstanceConf("charset").c_str(),sizeof(stDbConfDetail.m_stDbConnInfo.m_szDbCharSet));
    stDbConfDetail.m_stDbConnInfo.m_uiDbPort = atoi(oInstanceConf("port").c_str());
    stDbConfDetail.m_ucDbType = MYSQL_DB;
    stDbConfDetail.m_ucAccess = 1; //直连
    stDbConfDetail.m_stDbConnInfo.uiTimeOut = atoi(oInstanceConf("timeout").c_str());

    LOG4_DEBUG("InitDbConn(%s, %s, %s, %s, %u)", stDbConfDetail.m_stDbConnInfo.m_szDbHost,
                    stDbConfDetail.m_stDbConnInfo.m_szDbUser, stDbConfDetail.m_stDbConnInfo.m_szDbPwd,
                    stDbConfDetail.m_stDbConnInfo.m_szDbName, stDbConfDetail.m_stDbConnInfo.m_uiDbPort);
    iResult = pDbi->InitDbConn(stDbConfDetail);
    if (0 != iResult)
    {
        LOG4_ERROR("error %d: %s", pDbi->GetErrno(),pDbi->GetError().c_str());
    }
    return(iResult);
}

int CmdExecSql::Query(const neb::Mydis& oQuery, CMysqlDbi* pDbi)
{
    LOG4_TRACE("%s()", __FUNCTION__);
    int iResult = 0;
    MsgHead oOutMsgHead;
    MsgBody oOutMsgBody;
    neb::Result oRsp;
    oRsp.set_from(neb::Result::FROM_DB);
    neb::Record* pRecord = NULL;
    neb::Field* pField = NULL;

    if (NULL == pDbi)
    {
        LOG4_ERROR("pDbi is null!");
        Response(ERR_QUERY, "pDbi is null");
        return(ERR_QUERY);
    }

    std::string strSql;
    if (!CreateSql(oQuery, pDbi, strSql))
    {
        LOG4_ERROR("Scrabble up sql error!");
        Response(ERR_QUERY, strSql);
        return(ERR_QUERY);
    }
    LOG4_DEBUG("%s", strSql.c_str());
    iResult = pDbi->ExecSql(strSql);
    if (iResult == 0)
    {
        if (neb::Mydis::DbOperate::SELECT == oQuery.db_operate().query_type())
        {
            if (NULL != pDbi->UseResult())
            {
                uint32 uiDataLen = 0;
                uint32 uiRecordSize = 0;
                int32 iRecordNum = 0;
                unsigned int uiFieldNum = pDbi->FetchFieldNum();

                //字段值进行赋值
                MYSQL_ROW stRow;
                unsigned long* lengths;

                oRsp.set_err_no(neb::ERR_OK);
                oRsp.set_err_msg("success");
                while (NULL != (stRow = pDbi->GetRow()))
                {
                    uiRecordSize = 0;
                    ++iRecordNum;
                    lengths = pDbi->FetchLengths();
                    pRecord = oRsp.add_record_data();
                    for(unsigned int i = 0; i < uiFieldNum; ++i)
                    {
                        uiRecordSize += lengths[i];
                    }
                    if (uiRecordSize > 1000000)
                    {
                        LOG4_ERROR("error %d: %s", ERR_RESULTSET_EXCEED, "result set(one record) exceed 1 MB!");
                        Response(ERR_RESULTSET_EXCEED, "result set(one record) exceed 1 MB!");
                        return(ERR_RESULTSET_EXCEED);
                    }
                    if (uiDataLen + uiRecordSize > 1000000)
                    {
                        oRsp.set_current_count(iRecordNum);
                        oRsp.set_total_count(iRecordNum + 1);    // 表示未完
                        if (Response(oRsp))
                        {
                            oRsp.clear_record_data();
                            uiDataLen = 0;
                        }
                        else
                        {
                            return(ERR_RESULTSET_EXCEED);
                        }
                    }

                    for(unsigned int i = 0; i < uiFieldNum; ++i)
                    {
                        pField = pRecord->add_field_info();
                        if (0 == lengths[i])
                        {
                            pField->set_col_value("");
                        }
                        else
                        {
                            pField->set_col_value(stRow[i], lengths[i]);
                            uiDataLen += lengths[i];
                        }
                    }
                }
                oRsp.set_current_count(iRecordNum);
                oRsp.set_total_count(iRecordNum);

                Response(oRsp);
                return(pDbi->GetErrno());
            }
            else
            {
                Response(pDbi->GetErrno(), pDbi->GetError());
                LOG4_ERROR("%d: %s", pDbi->GetErrno(), pDbi->GetError().c_str());
                return(pDbi->GetErrno());
            }
        }
        else
        {
            Response(pDbi->GetErrno(), pDbi->GetError());
            LOG4_DEBUG("%d: %s", pDbi->GetErrno(), pDbi->GetError().c_str());
            return(pDbi->GetErrno());
        }
    }
    else
    {
        LOG4_ERROR("%s\t%d: %s", strSql.c_str(), pDbi->GetErrno(), pDbi->GetError().c_str());
        if (neb::Mydis::DbOperate::SELECT != oQuery.db_operate().query_type()
                        && ((pDbi->GetErrno() >= 2001 && pDbi->GetErrno() <= 2018)
                                        || (pDbi->GetErrno() >= 2038 && pDbi->GetErrno() <= 2046)))
        {   // 由于连接方面原因数据写失败，将失败数据节点返回给数据代理，等服务从故障中恢复后再由数据代理自动重试
            oRsp.set_err_no(pDbi->GetErrno());
            oRsp.set_err_msg(pDbi->GetError());
            neb::Result::DataLocate* pLocate = oRsp.mutable_locate();
            pLocate->set_section_from(m_uiSectionFrom);
            pLocate->set_section_to(m_uiSectionTo);
            pLocate->set_hash(m_uiHash);
            pLocate->set_divisor(m_uiDivisor);
            Response(oRsp);
        }
        else
        {
            Response(pDbi->GetErrno(), pDbi->GetError());
        }
    }

    return(iResult);
}

void CmdExecSql::CheckConnection()
{
    LOG4_TRACE("%s()",__FUNCTION__);
//    time_t lNowTime = time(NULL);

    //长连接。检查mapdbi中的连接实例有无超时的，超时的连接删除
//    std::map<std::string,tagConnection*>::iterator conn_iter;
//    for (conn_iter = m_mapDbiPool.begin(); conn_iter != m_mapDbiPool.end(); )
//    {
//        int iTimeOut = m_iConnectionTimeout;
//        if (conn_iter->second->iTimeout > 0)
//        {
//            iTimeOut = conn_iter->second->iTimeout;
//        }
//
//        int iDiffTime = lNowTime - conn_iter->second->ullBeatTime;  //求时差
//        if (iDiffTime > iTimeOut)
//        {
//            if (NULL != conn_iter->second)
//            {
//                delete conn_iter->second;
//                conn_iter->second = NULL;
//            }
//            m_mapDbiPool.erase(conn_iter);
//        }
//        else
//        {
//            ++conn_iter;
//        }
//    }
}

void CmdExecSql::Response(int iErrno, const std::string& strErrMsg)
{
    LOG4_TRACE("error %d: %s", iErrno, strErrMsg.c_str());
    MsgBody oOutMsgBody;
    neb::Result oRsp;
    oRsp.set_from(neb::Result::FROM_DB);
    oRsp.set_err_no(iErrno);
    oRsp.set_err_msg(strErrMsg);
    oOutMsgBody.set_data(oRsp.SerializeAsString());
    SendTo(m_pChannel, m_oInMsgHead.cmd() + 1, m_oInMsgHead.seq(), oOutMsgBody);
}

bool CmdExecSql::Response(const neb::Result& oRsp)
{
    LOG4_TRACE("error %d: %s", oRsp.err_no(), oRsp.err_msg().c_str());
    MsgBody oOutMsgBody;
    oOutMsgBody.set_data(oRsp.SerializeAsString());
    return(SendTo(m_pChannel, m_oInMsgHead.cmd() + 1, m_oInMsgHead.seq(), oOutMsgBody));
}

std::string CmdExecSql::GetFullTableName(const std::string& strTableName, uint32 uiFactor)
{
    char szFullTableName[128] = {0};
    std::string strDbName = m_oDbConf["table"][strTableName]("db_name");
    int iTableNum = atoi(m_oDbConf["table"][strTableName]("table_num").c_str());
    if (1 == iTableNum)
    {
        snprintf(szFullTableName, sizeof(szFullTableName), "%s.%s", strDbName.c_str(), strTableName.c_str());
    }
    else
    {
        uint32 uiTableIndex = uiFactor % iTableNum;
        snprintf(szFullTableName, sizeof(szFullTableName), "%s.%s_%02d", strDbName.c_str(), strTableName.c_str(), uiTableIndex);
        m_uiHash = uiTableIndex;
        m_uiDivisor = iTableNum;
    }
    return(szFullTableName);
}

bool CmdExecSql::CreateSql(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strSql.clear();
    bool bResult = false;
    for (unsigned int i = 0; i < oQuery.db_operate().table_name().size(); ++i)
    {
        if ((oQuery.db_operate().table_name()[i] >= 'A' && oQuery.db_operate().table_name()[i] <= 'Z')
            || (oQuery.db_operate().table_name()[i] >= 'a' && oQuery.db_operate().table_name()[i] <= 'z')
            || (oQuery.db_operate().table_name()[i] >= '0' && oQuery.db_operate().table_name()[i] <= '9')
            || oQuery.db_operate().table_name()[i] == '_' || oQuery.db_operate().table_name()[i] == '.')
        {
            ;
        }
        else
        {
            return(false);
        }
    }

    switch (oQuery.db_operate().query_type())
    {
        case neb::Mydis::DbOperate::SELECT:
        {
            bResult = CreateSelect(oQuery, strSql);
            break;
        }
        case neb::Mydis::DbOperate::INSERT:
        case neb::Mydis::DbOperate::INSERT_IGNORE:
        case neb::Mydis::DbOperate::REPLACE:
        {
            bResult = CreateInsert(oQuery, pDbi, strSql);
            break;
        }
        case neb::Mydis::DbOperate::UPDATE:
        {
            bResult = CreateUpdate(oQuery, pDbi, strSql);
            break;
        }
        case neb::Mydis::DbOperate::DELETE:
        {
            bResult = CreateDelete(oQuery, strSql);
            break;
        }
        default:
            LOG4_ERROR("invalid query_type %d", oQuery.db_operate().query_type());
            break;
    }

    if (oQuery.db_operate().conditions_size() > 0)
    {
        std::string strCondition;
        bResult = CreateConditionGroup(oQuery, pDbi, strCondition);
        if (bResult)
        {
            strSql += std::string(" WHERE ") + strCondition;
        }
    }

    if (oQuery.db_operate().groupby_col_size() > 0)
    {
        std::string strGroupBy;
        bResult = CreateGroupBy(oQuery, strGroupBy);
        if (bResult)
        {
            strSql += std::string(" GROUP BY ") + strGroupBy;
        }
    }

    if (oQuery.db_operate().orderby_col_size() > 0)
    {
        std::string strOrderBy;
        bResult = CreateOrderBy(oQuery, strOrderBy);
        if (bResult)
        {
            strSql += std::string(" ORDER BY ") + strOrderBy;
        }
    }

    if (oQuery.db_operate().limit() > 0)
    {
        std::string strLimit;
        bResult = CreateLimit(oQuery, strLimit);
        if (bResult)
        {
            strSql += std::string(" LIMIT ") + strLimit;
        }
    }

    return(bResult);
}

bool CmdExecSql::CreateSelect(const neb::Mydis& oQuery, std::string& strSql)
{
    strSql = "SELECT ";
    for (int i = 0; i < oQuery.db_operate().fields_size(); ++i)
    {
        if (!CheckColName(oQuery.db_operate().fields(i).col_name()))
        {
            LOG4_ERROR("invalidcol_name \"%s\".", oQuery.db_operate().fields(i).col_name().c_str());
            return(false);
        }
        if (i == 0)
        {
            if (oQuery.db_operate().fields(i).col_as().length() > 0)
            {
                strSql += oQuery.db_operate().fields(i).col_name() + std::string(" AS ") + oQuery.db_operate().fields(i).col_as();
            }
            else
            {
                strSql += oQuery.db_operate().fields(i).col_name();
            }
        }
        else
        {
            if (oQuery.db_operate().fields(i).col_as().length() > 0)
            {
                strSql += std::string(",") + oQuery.db_operate().fields(i).col_name() + std::string(" AS ") + oQuery.db_operate().fields(i).col_as();
            }
            else
            {
                strSql += std::string(",") + oQuery.db_operate().fields(i).col_name();
            }
        }
    }

    std::string strTableName;
    if (0 == oQuery.db_operate().mod_factor())
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.section_factor());
    }
    else
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.db_operate().mod_factor());
    }
    if (strTableName.empty())
    {
        LOG4_ERROR("dbname_table is NULL");
        return false;
    }

    strSql += std::string(" FROM ") + strTableName;

    return true;
}

bool CmdExecSql::CreateInsert(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strSql = "";
    if (neb::Mydis::DbOperate::INSERT == oQuery.db_operate().query_type())
    {
        strSql = "INSERT INTO ";
    }
    else if (neb::Mydis::DbOperate::INSERT_IGNORE == oQuery.db_operate().query_type())
    {
        strSql = "INSERT IGNORE INTO ";
    }
    else if (neb::Mydis::DbOperate::REPLACE == oQuery.db_operate().query_type())
    {
        strSql = "REPLACE INTO ";
    }

    std::string strTableName;
    if (0 == oQuery.db_operate().mod_factor())
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.section_factor());
    }
    else
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.db_operate().mod_factor());
    }
    if (strTableName.empty())
    {
        LOG4_ERROR("dbname_tbname is null");
        return false;
    }

    strSql += strTableName;

    for (int i = 0; i < oQuery.db_operate().fields_size(); ++i)
    {
        if (!CheckColName(oQuery.db_operate().fields(i).col_name()))
        {
            return(false);
        }
        if (i == 0)
        {
            strSql += std::string("(") + oQuery.db_operate().fields(i).col_name();
        }
        else
        {
            strSql += std::string(",") + oQuery.db_operate().fields(i).col_name();
        }
    }
    strSql += std::string(") ");

    for (int i = 0; i < oQuery.db_operate().fields_size(); ++i)
    {
        if (i == 0)
        {
            if (neb::STRING == oQuery.db_operate().fields(i).col_type())
            {
                pDbi->EscapeString(m_szColValue, oQuery.db_operate().fields(i).col_value().c_str(), oQuery.db_operate().fields(i).col_value().size());
                strSql += std::string("VALUES('") + std::string(m_szColValue) + std::string("'");
            }
            else
            {
                for (unsigned int j = 0; j < oQuery.db_operate().fields(i).col_value().size(); ++j)
                {
                    if (oQuery.db_operate().fields(i).col_value()[j] >= '0' || oQuery.db_operate().fields(i).col_value()[j] <= '9'
                        || oQuery.db_operate().fields(i).col_value()[j] == '.')
                    {
                        ;
                    }
                    else
                    {
                        return(false);
                    }
                }
                strSql += std::string("VALUES(") + oQuery.db_operate().fields(i).col_value();
            }
        }
        else
        {
            if (neb::STRING == oQuery.db_operate().fields(i).col_type())
            {
                pDbi->EscapeString(m_szColValue, oQuery.db_operate().fields(i).col_value().c_str(), oQuery.db_operate().fields(i).col_value().size());
                strSql += std::string(",'") + std::string(m_szColValue) + std::string("'");
            }
            else
            {
                for (unsigned int j = 0; j < oQuery.db_operate().fields(i).col_value().size(); ++j)
                {
                    if (oQuery.db_operate().fields(i).col_value()[j] >= '0' || oQuery.db_operate().fields(i).col_value()[j] <= '9'
                        || oQuery.db_operate().fields(i).col_value()[j] == '.')
                    {
                        ;
                    }
                    else
                    {
                        return(false);
                    }
                }
                strSql += std::string(",") + oQuery.db_operate().fields(i).col_value();
            }
        }
    }
    strSql += std::string(")");

    return true;
}

bool CmdExecSql::CreateUpdate(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strSql)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strSql = "UPDATE ";
    std::string strTableName;
    if (0 == oQuery.db_operate().mod_factor())
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.section_factor());
    }
    else
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.db_operate().mod_factor());
    }
    if (strTableName.empty())
    {
        LOG4_ERROR("dbname_tbname is null");
        return false;
    }

    strSql += strTableName;

    for (int i = 0; i < oQuery.db_operate().fields_size(); ++i)
    {
        if (!CheckColName(oQuery.db_operate().fields(i).col_name()))
        {
            return(false);
        }
        if (i == 0)
        {
            if (neb::STRING == oQuery.db_operate().fields(i).col_type())
            {
                pDbi->EscapeString(m_szColValue, oQuery.db_operate().fields(i).col_value().c_str(), oQuery.db_operate().fields(i).col_value().size());
                strSql += std::string(" SET ") + oQuery.db_operate().fields(i).col_name() + std::string("=");
                strSql += std::string("'") + std::string(m_szColValue) + std::string("'");
            }
            else
            {
                for (unsigned int j = 0; j < oQuery.db_operate().fields(i).col_value().size(); ++j)
                {
                    if (oQuery.db_operate().fields(i).col_value()[j] >= '0' || oQuery.db_operate().fields(i).col_value()[j] <= '9'
                        || oQuery.db_operate().fields(i).col_value()[j] == '.')
                    {
                        ;
                    }
                    else
                    {
                        return(false);
                    }
                }
                strSql += std::string(" SET ") + oQuery.db_operate().fields(i).col_name()
                    + std::string("=") + oQuery.db_operate().fields(i).col_value();
            }
        }
        else
        {
            if (neb::STRING == oQuery.db_operate().fields(i).col_type())
            {
                pDbi->EscapeString(m_szColValue, oQuery.db_operate().fields(i).col_value().c_str(), oQuery.db_operate().fields(i).col_value().size());
                strSql += std::string(", ") + oQuery.db_operate().fields(i).col_name() + std::string("=");
                strSql += std::string("'") + std::string(m_szColValue) + std::string("'");
            }
            else
            {
                for (unsigned int j = 0; j < oQuery.db_operate().fields(i).col_value().size(); ++j)
                {
                    if (oQuery.db_operate().fields(i).col_value()[j] >= '0' || oQuery.db_operate().fields(i).col_value()[j] <= '9'
                        || oQuery.db_operate().fields(i).col_value()[j] == '.')
                    {
                        ;
                    }
                    else
                    {
                        return(false);
                    }
                }
                strSql += std::string(", ") + oQuery.db_operate().fields(i).col_name()
                    + std::string("=") + oQuery.db_operate().fields(i).col_value();
            }
        }
    }

    return true;
}

bool CmdExecSql::CreateDelete(const neb::Mydis& oQuery, std::string& strSql)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strSql = "DELETE FROM ";
    std::string strTableName;
    if (0 == oQuery.db_operate().mod_factor())
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.section_factor());
    }
    else
    {
        strTableName = GetFullTableName(oQuery.db_operate().table_name(), oQuery.db_operate().mod_factor());
    }
    if (strTableName.empty())
    {
        LOG4_ERROR("dbname_tbname is null");
        return false;
    }

    strSql += strTableName;

    return true;
}

bool CmdExecSql::CreateCondition(const neb::Mydis::DbOperate::Condition& oCondition, CMysqlDbi* pDbi, std::string& strCondition)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    if (!CheckColName(oCondition.col_name()))
    {
        return(false);
    }
    strCondition = oCondition.col_name();
    switch (oCondition.relation())
    {
    case neb::Mydis::DbOperate::Condition::EQ:
        strCondition += std::string("=");
        break;
    case neb::Mydis::DbOperate::Condition::NE:
        strCondition += std::string("<>");
        break;
    case neb::Mydis::DbOperate::Condition::GT:
        strCondition += std::string(">");
        break;
    case neb::Mydis::DbOperate::Condition::LT:
        strCondition += std::string("<");
        break;
    case neb::Mydis::DbOperate::Condition::GE:
        strCondition += std::string(">=");
        break;
    case neb::Mydis::DbOperate::Condition::LE:
        strCondition += std::string("<=");
        break;
    case neb::Mydis::DbOperate::Condition::LIKE:
        strCondition += std::string(" LIKE ");
        break;
    case neb::Mydis::DbOperate::Condition::IN:
        strCondition += std::string(" IN (");
        break;
    default:
        break;
    }

    if (oCondition.col_name_right().length() > 0)
    {
        if (!CheckColName(oCondition.col_name_right()))
        {
            return(false);
        }
        strCondition += oCondition.col_name_right();
    }
    else
    {
        for (int i = 0; i < oCondition.col_values_size(); ++i)
        {
            if (i > 0)
            {
                strCondition += std::string(",");
            }

            if (neb::STRING == oCondition.col_type())
            {
                pDbi->EscapeString(m_szColValue, oCondition.col_values(i).c_str(), oCondition.col_values(i).size());
                strCondition += std::string("'") + std::string(m_szColValue) + std::string("'");
            }
            else
            {
                for (unsigned int j = 0; j < oCondition.col_values(i).size(); ++j)
                {
                    if (oCondition.col_values(i)[j] >= '0' || oCondition.col_values(i)[j] <= '9'
                        || oCondition.col_values(i)[j] == '.')
                    {
                        ;
                    }
                    else
                    {
                        return(false);
                    }
                }
                strCondition += oCondition.col_values(i);
            }
        }

        if (neb::Mydis::DbOperate::Condition::IN == oCondition.relation())
        {
            strCondition += std::string(")");
        }
    }

    return true;
}

bool CmdExecSql::CreateConditionGroup(const neb::Mydis& oQuery, CMysqlDbi* pDbi, std::string& strCondition)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    bool bResult = false;
    for (int i = 0; i < oQuery.db_operate().conditions_size(); ++i)
    {
        if (i > 0)
        {
            if (neb::Mydis::DbOperate::ConditionGroup::OR == oQuery.db_operate().group_relation())
            {
                strCondition += std::string(" OR ");
            }
            else
            {
                strCondition += std::string(" AND ");
            }
        }
        if (oQuery.db_operate().conditions_size() > 1)
        {
            strCondition += std::string("(");
        }
        for (int j = 0; j < oQuery.db_operate().conditions(i).condition_size(); ++j)
        {
            std::string strRelation;
            std::string strOneCondition;
            if (neb::Mydis::DbOperate::ConditionGroup::OR == oQuery.db_operate().conditions(i).relation())
            {
                strRelation = " OR ";
            }
            else
            {
                strRelation = " AND ";
            }
            bResult = CreateCondition(oQuery.db_operate().conditions(i).condition(j), pDbi, strOneCondition);
            if (bResult)
            {
                if (j > 0)
                {
                    strCondition += strRelation;
                }
                strCondition += strOneCondition;
            }
            else
            {
                return(bResult);
            }
        }
        if (oQuery.db_operate().conditions_size() > 1)
        {
            strCondition += std::string(")");
        }
    }

    return true;
}

bool CmdExecSql::CreateGroupBy(const neb::Mydis& oQuery, std::string& strGroupBy)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strGroupBy = "";
    for (int i = 0; i < oQuery.db_operate().groupby_col_size(); ++i)
    {
        if (!CheckColName(oQuery.db_operate().groupby_col(i)))
        {
            return(false);
        }
        if (i == 0)
        {
            strGroupBy += oQuery.db_operate().groupby_col(i);
        }
        else
        {
            strGroupBy += std::string(",") + oQuery.db_operate().groupby_col(i);
        }
    }

    return true;
}

bool CmdExecSql::CreateOrderBy(const neb::Mydis& oQuery, std::string& strOrderBy)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    strOrderBy = "";
    for (int i = 0; i < oQuery.db_operate().orderby_col_size(); ++i)
    {
        if (!CheckColName(oQuery.db_operate().orderby_col(i).col_name()))
        {
            return(false);
        }
        if (i == 0)
        {
            if (neb::Mydis::DbOperate::OrderBy::DESC == oQuery.db_operate().orderby_col(i).relation())
            {
                strOrderBy += oQuery.db_operate().orderby_col(i).col_name() + std::string(" DESC");
            }
            else
            {
                strOrderBy += oQuery.db_operate().orderby_col(i).col_name() + std::string(" ASC");
            }
        }
        else
        {
            if (neb::Mydis::DbOperate::OrderBy::DESC == oQuery.db_operate().orderby_col(i).relation())
            {
                strOrderBy += std::string(",") + oQuery.db_operate().orderby_col(i).col_name() + std::string(" DESC");
            }
            else
            {
                strOrderBy += std::string(",") + oQuery.db_operate().orderby_col(i).col_name() + std::string(" ASC");
            }
        }
    }

    return true;
}

bool CmdExecSql::CreateLimit(const neb::Mydis& oQuery, std::string& strLimit)
{
    LOG4_TRACE("%s()",__FUNCTION__);

    char szLimit[16] = {0};
    if (0 == oQuery.db_operate().limit_from())
    {
        snprintf(szLimit, sizeof(szLimit), "%u", oQuery.db_operate().limit());
        strLimit = szLimit;
    }
    else
    {
        snprintf(szLimit, sizeof(szLimit), "%u,%u", oQuery.db_operate().limit_from(), oQuery.db_operate().limit());
        strLimit = szLimit;
    }
    return true;
}

bool CmdExecSql::CheckColName(const std::string& strColName)
{
    std::string strUpperColName;
    for (unsigned int i = 0; i < strColName.size(); ++i)
    {
        if (strColName[i] == '\'' || strColName[i] == '"' || strColName[i] == '\\'
            || strColName[i] == ';' || strColName[i] == '*' || strColName[i] == ' ')
        {
            return(false);
        }

        if (strColName[i] >= 'a' && strColName[i] <= 'z')
        {
            strUpperColName[i] = strColName[i] - 32;
        }
        else
        {
            strUpperColName[i] = strColName[i];
        }
    }

    if (strUpperColName == std::string("AND") || strUpperColName == std::string("OR")
        || strUpperColName == std::string("UPDATE") || strUpperColName == std::string("DELETE")
        || strUpperColName == std::string("UNION") || strUpperColName == std::string("AS")
        || strUpperColName == std::string("CHANGE") || strUpperColName == std::string("SET")
        || strUpperColName == std::string("TRUNCATE") || strUpperColName == std::string("DESC"))
    {
        return(false);
    }
    return true;
}


} /* namespace oss */
