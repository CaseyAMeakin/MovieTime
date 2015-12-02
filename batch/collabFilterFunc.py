"""
Functional style implemention of collaborative filtering subroutines
"""

import MySQLdb as mdb
import numpy as np


def connectMySql(db,dbuser,dbpw):
    """
    """
    if not self.cur:
        try:
            con = mdb.connect(db=db,user=dbuser,passwd=dbpw)
        except:
            print 'Trouble connecting to MySQL db'
            con = None
        return con
        
def tryMySqlFetchall(con,sqlcmd):
    """
    """
    try:
        cur     = con.cursor()
        curcall = cur.execute(sqlcmd)
        query   = cur.fetchall()
    except:
        query    = None
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
    matrix_size_critic = np.array([np.max(criticIds),np.max(movieIds)],dtype='i')                                          
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
        vote_matrix[int(cid-1),:] = thisRow

    return vote_matrix


def getVoteMatrixUser(con,userIds,movieIds,nullVoteValue=-99.,Quiet=True):
    """
    """
    matrix_size_user = np.array([np.max(userIds),np.max(movieIds)],dtype='i')
    vote_matrix_user = np.zeros(list(matrix_size_user),dtype='f') + nullVoteValue   

    sqlcmd_ = u"""select uv.itemId,uv.like from movies_site.movies_uservotes as uv """ + \
              u"""where uv.itemType=1 and uv.user_id={0}"""        
    
    for uid in self.userIds:
        sqlcmd = sqlcmd_.format(uid)
        query = tryMysqlFetchall(con,sqlcmd)
        mids  = [int(item[0] - 1) for item in query]
        votes = np.array([float(item[1]) for item in query],dtype='i')
        if list( np.where(votes == nullVoteValue))[0]:
            print 'Error: there is vote data that has value == nullVoteValue'
            return vote_matrix_user
        thisRow = np.zeros(self.matrix_size_user[1],dtype='f') + self.nullVoteValue
        thisRow[mids] = votes
        self.vote_matrix_user[int(uid-1),:] = thisRow

    return vote_matrix_user


def calculateWeightVectorUser(uid,vote_matrix_user,vote_matrix_critic,criticIds,nullVoteValue=-99.):
    """
    """
    weight_matrix_shape = (np.shape(vote_matrix_user)[0],np.shape(vote_matrix_critic)[0])
    weight_matrix = np.zeros( np.shape(vote_matrix_critic)[0],dtype='f')

    for cid in criticIds:
        cRow = vote_matrix_critic[cid-1,:]
        cCount = np.array(np.where(cRow != nullVoteValue),dtype='i')
        uRow = vote_matrix_user[uid-1,:]
        uCount = np.array(np.where(uRow != nullVoteValue),dtype='i')
        toCount = np.intersect1d(cCount,uCount)
        a = cRow[toCount] - np.mean(cRow[cCount])
        b = uRow[toCount] - np.mean(uRow[uCount])
        num = np.dot(a,b)
        den = np.sqrt(np.dot(a,a)*np.dot(b,b))
        if den != 0.0: weight_matrix[uid-1,cid-1] = (num/den)

    return weight_matrix


def getOverlappingCriticIds(con,uid,criticIds):
    """
    """
    overlappingCriticIds = []
    
    sqlcmd_ = [u"""select r.criticid from movies_site.movies_uservotes as uv """ + \
               u"""join rt.reviews as r on r.movieid = uv.itemId """ + \
               u"""where uv.user_id = {0} """ + \
               u"""group by r.criticid having count(r.movieid) > {1} order by r.criticid asc """,
               u"""select count(uv.itemId) from movies_site.movies_uservotes as uv """ + \
               u"""where uv.itemType=1 and uv.user_id = {0}"""]

    sqlcmd = sqlcmd_[1].format(uid)
    query = tryMysqlFetchall(con,sqlcmd)
    nmovies = query[0][0]
    noverlap =max(10,int(np.ceil(0.1*nmovies)))
    sqlcmd = sqlcmd_[0].format(uid,noverlap)
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


def calculatePredictedMovieVotes(uid,weight_vector,overlappingCriticIds,poolMoviesIds, \
                                 nullVoteValue=-99.,minCritics=10):
    """
    """
    predictedMovieVotes = []
    cids = np.array([ cid - 1 for cid in overlappingCriticIds],dtype='i')
    movieIndex = poolMovieIds
    abs_wjk = np.abs(weight_matrix[uid-1,:])

    wrow = weight_matrix[uid-1,:]
    wrow = wrow[cids]

    avgUserVote = vote_user_avg[uid-1]
    predictedVotes = []

    for mid in self.poolMovieIds:

        crow = vote_matrix_critic[:,mid-1]
        crow = crow[cids]
        toCount = np.array(np.where(crow != self.nullVoteValue),dtype='i')[0]
        if len(toCount) < minCritics:
            predictedVote = -1.
        else:
            num = np.dot(crow[toCount],wrow[toCount])
            den = np.sum(abs_wjk[cids[toCount]])
            predictedVote = 0.
            if den != 0: predictedVote += num/den

        predictedVotes.append(predictedVote)
        predictedMovieVotes[uid-1] = predictedVotes

    return predictedMovieVotes
