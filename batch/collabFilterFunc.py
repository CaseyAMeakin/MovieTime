"""
Functional style implemention of collaborative filtering subroutines
"""

import MySQLdb as mdb
import numpy as np


def connectMysql(db,dbuser,dbpw):
    """
    """
    try:
        con = mdb.connect(db=db,user=dbuser,passwd=dbpw)
    except:
        print 'Trouble connecting to MySQL db'
        con = None
    return con
        
def tryMysqlFetchall(con,sqlcmd):
    """
    """
    try:
        cur     = con.cursor()
        curcall = cur.execute(sqlcmd)
        query   = cur.fetchall()
    except:
        query    = []
    return query


def getCriticIds(con):
    """
    """
    sqlcmd = u"""select distinct r.criticid from rt.reviews as r order by criticid"""
    query = tryMysqlFetchall(con,sqlcmd)
    criticIds = np.array([])
    if query: 
        try:
            criticIds = np.array([item[0] for item in query],dtype='i')
        except:
            pass
    return criticIds


def getMovieIds(con):
    """
    """
    sqlcmd = u"""select distinct r.movieid from rt.reviews as r order by r.movieid"""
    query = tryMysqlFetchall(con,sqlcmd)
    movieIds = np.array([])
    if query:
        try:
            movieIds = np.array([item[0] for item in query],dtype='i')
        except:
            pass
    return movieIds


def getUserIds(con):
    """
    """
    sqlcmd = u"""select distinct uv.user_id from movies_site.movies_uservotes as uv order by user_id"""
    query = tryMysqlFetchall(con,sqlcmd)
    userIds = np.array([])
    if query:
        try:
            userIds = np.array([item[0] for item in query],dtype='i')
        except:
            pass
    return userIds


def getVoteMatrixCritic(con,criticIds,movieIds,nullVoteValue=-99.,quiet=True):  
    """
    """
    matrix_size        = np.array([np.max(criticIds),np.max(movieIds)],dtype='i')                                          
    vote_matrix_critic = np.zeros(matrix_size,dtype='f') + nullVoteValue

    sqlcmd_ = u"""select r.movieid,r.fresh from rt.reviews as r where r.criticid={0}"""

    for cid in criticIds:
        sqlcmd = sqlcmd_.format(cid)
        query = tryMysqlFetchall(con,sqlcmd)
        mids  = [int(item[0] - 1)   for item in query]
        votes = np.array([float(item[1]) for item in query],dtype='i')
        if list( np.where(votes == nullVoteValue))[0]:
            print 'Error: there is vote data that has value == nullVoteValue'
            return vote_matrix

        thisRow = np.zeros(matrix_size[1],dtype='f') + nullVoteValue
        thisRow[mids] = votes
        vote_matrix_critic[int(cid-1),:] = thisRow

    return vote_matrix_critic


def getVoteMatrixUser(con,userIds,movieIds,nullVoteValue=-99.,Quiet=True):
    """
    """
    matrix_size_user = np.array([np.max(userIds),np.max(movieIds)],dtype='i')
    vote_matrix_user = np.zeros(list(matrix_size_user),dtype='f') + nullVoteValue   

    sqlcmd_ = u"""select uv.itemId,uv.like from movies_site.movies_uservotes as uv """ + \
              u"""where uv.itemType=1 and uv.user_id={0}"""        
    
    for uid in userIds:
        sqlcmd = sqlcmd_.format(uid)
        query = tryMysqlFetchall(con,sqlcmd)
        mids  = [int(item[0] - 1) for item in query]
        votes = np.array([float(item[1]) for item in query],dtype='i')
        if list( np.where(votes == nullVoteValue))[0]:
            print 'Error: there is vote data that has value == nullVoteValue'
            return vote_matrix_user
        thisRow = np.zeros(matrix_size_user[1],dtype='f') +nullVoteValue
        thisRow[mids] = votes
        vote_matrix_user[int(uid-1),:] = thisRow

    return vote_matrix_user


def getVoteVectorUser(con,uid,movieIds,nullVoteValue=-99.,Quiet=True):
    """
    """
    vector_length = np.max(movieIds)
    vote_vector_user = np.zeros(vector_length,dtype='f') + nullVoteValue

    sqlcmd_ = u"""select uv.itemId,uv.like from movies_site.movies_uservotes as uv """ + \
              u"""where uv.itemType=1 and uv.user_id={0}"""
    
    sqlcmd = sqlcmd_.format(uid)
    query = tryMysqlFetchall(con,sqlcmd)
    mids  = [int(item[0] - 1) for item in query]
    votes = np.array([float(item[1]) for item in query],dtype='i')
    if list( np.where(votes == nullVoteValue))[0]:
        print 'Error: there is vote data that has value == nullVoteValue'
        return vote_matrix_user
    thisRow = np.zeros(vector_length,dtype='f') +nullVoteValue
    thisRow[mids] = votes
    vote_vector_user = thisRow

    return vote_vector_user


def calculateWeightVectorUser(uid,vote_vector_user,vote_matrix_critic,criticIds,nullVoteValue=-99.):
    """
    """
    weight_vector = np.zeros( np.shape(vote_matrix_critic)[0],dtype='f')

    for cid in criticIds:
        cRow = vote_matrix_critic[cid-1,:]
        cCount = np.array(np.where(cRow != nullVoteValue),dtype='i')
        uRow = vote_vector_user
        uCount = np.array(np.where(uRow != nullVoteValue),dtype='i')
        toCount = np.intersect1d(cCount,uCount)
        a = cRow[toCount] - np.mean(cRow[cCount])
        b = uRow[toCount] - np.mean(uRow[uCount])
        num = np.dot(a,b)
        den = np.sqrt(np.dot(a,a)*np.dot(b,b))
        if den != 0.0: weight_vector[cid-1] = (num/den)

    return weight_vector


def getOverlappingCriticIds(con,uid,criticIds,noverlap_min=10):
    """
    """
    overlappingCriticIds = []
    
    sqlcmd_ = u"""select count(uv.itemId) from movies_site.movies_uservotes as uv """ + \
              u"""where uv.itemType=1 and uv.user_id = {0}"""
    
    sqlcmd = sqlcmd_.format(uid)
    query = tryMysqlFetchall(con,sqlcmd)
    nmovies = query[0][0]
    noverlap =max(noverlap_min,int(np.ceil(0.1*nmovies)))

    sqlcmd_ = u"""select r.criticid from movies_site.movies_uservotes as uv """ + \
              u"""join rt.reviews as r on r.movieid = uv.itemId """ + \
              u"""where uv.user_id = {0} and uv.itemType = 1 """ + \
              u"""group by r.criticid having count(r.movieid) > {1} order by r.criticid asc """
    sqlcmd = sqlcmd_.format(uid,noverlap)
    query = tryMysqlFetchall(con,sqlcmd)
    overlapList = [item[0] for item in query]
    overlappingCriticIds = overlapList

    return overlappingCriticIds


def getPoolMovieIds(con,uid,overlappingCriticIds):
    """
    """
    poolMovieIds = []
    mids = []

    sqlcmd_ = u"""select distinct r.movieid from rt.reviews as r where r.criticid = {0}"""

    for cid in overlappingCriticIds:
        sqlcmd = sqlcmd_.format(cid)
        query = tryMysqlFetchall(con,sqlcmd)
        mids = list(set( [item[0] for item in query] + mids))        

    sqlcmd_ = u"""select distinct uv.itemId from movies_site.movies_uservotes as uv """ + \
              u"""where uv.itemType = 1 and uv.user_id = {0}"""
    sqlcmd = sqlcmd_.format(uid)
    query = tryMysqlFetchall(con,sqlcmd)
    umids = [item[0] for item in query]
    poolMovieIds = list(set(mids) - set(umids))
    return poolMovieIds


def calculatePredictedMovieVotes(uid,weight_vector,vote_vector_user,vote_matrix_critic,overlappingCriticIds,poolMovieIds, \
                                 nullVoteValue=-99.,minCritics=10):
    """
    """
    cids = np.array([ cid - 1 for cid in overlappingCriticIds],dtype='i')
    movieIndex = poolMovieIds
    abs_wjk = np.abs(weight_vector)

    wrow = weight_vector
    wrow = wrow[cids]

    avgUserVote = np.mean(vote_vector_user[np.where(vote_vector_user != nullVoteValue)])
    predictedVotes = []

    predictedMovieVotes = []
    for mid in poolMovieIds:

        crow = vote_matrix_critic[:,mid-1]
        crow = crow[cids]
        toCount = np.array(np.where(crow != nullVoteValue),dtype='i')[0]
        if len(toCount) < minCritics:
            predictedVote = -1.
        else:
            num = np.dot(crow[toCount],wrow[toCount])
            den = np.sum(abs_wjk[cids[toCount]])
            predictedVote = 0.
            if den != 0: predictedVote += num/den

        predictedVotes.append(predictedVote)

    predictedMovieVotes = predictedVotes
    return predictedMovieVotes
