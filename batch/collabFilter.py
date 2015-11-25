import MySQLdb as mdb
import numpy as np


class collabFilter(object):

    def __init__(self,db='rt',dbuser='root',dbpw='',debug=False,nullVoteValue=-99.):
        self.debug = debug
        self.db = db
        self.dbuser = dbuser
        self.dbpw = dbpw
        self.con = None
        self.cur = None

        emptyIntArray    = np.array([],dtype='i')
        emptyFloatArray  = np.array([],dtype='f')

        self.nullVoteValue = nullVoteValue

        self.movieIds    = emptyIntArray
        self.criticIds   = emptyIntArray
        self.matrix_size = emptyIntArray
        self.vote_matrix_loaded = False        
        self.vote_matrix = emptyFloatArray
        self.vote_critic_avg = emptyFloatArray

        self.userIds = emptyIntArray
        self.matrix_size_user = emptyIntArray
        self.vote_matrix_user_loeaded = False
        self.vote_matrix_user = emptyFloatArray
        self.vote_user_avg    = emptyFloatArray

        self.weight_matrix = emptyFloatArray
        self.overlappingCriticIds = []
        self.poolMovieIds = []
        self.predictedMovieVote = []


    def getCursor(self):
        if not self.cur:
            try:
                self.con = mdb.connect(db=self.db,user=self.dbuser,passwd=self.dbpw)
                self.cur = self.con.cursor()
            except:
                print 'Trouble connecting to MySQL db'
                self.con = None
                self.cur = None

    def trySqlFetchall(self,sqlcmd):
        if not self.cur: self.getCursor()
        if not self.cur: return None
        try:
            curcall = self.cur.execute(sqlcmd)
            query = self.cur.fetchall()
        except:
            query = None
        return query


    # Critic Data

    def getMatrixSize(self):        
        if not self.criticIds.any(): self.getCriticIds() 
        if not self.movieIds.any(): self.getMovieIds()
        if not self.movieIds.any() or not self.criticIds.any():
            print 'Error(getMatrixSize): trouble getting movieIds or criticIds'
            return
        else:
            self.matrix_size = np.array([np.max(self.criticIds),np.max(self.movieIds)],dtype='i')    
            return

    def getCriticIds(self):
        sqlcmd = u"""select distinct r.criticid from rt.reviews as r order by criticid"""
        query = self.trySqlFetchall(sqlcmd)
        if query: 
            try:
                self.criticIds = np.array([item[0] for item in query],dtype='i')
            except:
                pass

    def getMovieIds(self):
        sqlcmd = u"""select distinct r.movieid from rt.reviews as r order by r.movieid"""
        query = self.trySqlFetchall(sqlcmd)
        if query:
            try:
                self.movieIds = np.array([item[0] for item in query],dtype='i')
            except:
                pass

    def allocateVoteMatrix(self):
        self.getMatrixSize()
        if not self.matrix_size.any():
            print 'Error(initMatrix): matrix_size'
            return
        try:
            self.vote_matrix = np.zeros(list(self.matrix_size),dtype='f') + self.nullVoteValue
        except:
            pass

    def loadVoteMatrix(self,getAverageVotes=False,centerVotes=False):
        if self.debug==True: print 'Call loadVoteMatrix'
        if not len(self.vote_matrix): self.allocateVoteMatrix()
        if not len(self.criticIds): self.getCriticIds()
        if not len(self.movieIds): self.getMovieIds()
        if not len(self.vote_matrix):
            print 'Error(loadVoteMatrix): trouble allocating vote_matrix'
            return
        if not len(self.criticIds) or not len(self.movieIds):
            print 'Error(loadVoteMatrix): trouble getting criticIds or movieIds'
            return 

        if getAverageVotes or centerVotes:
            self.vote_critic_avg = np.zeros(self.matrix_size[0],dtype='f') + self.nullVoteValue

        sqlcmd_ = u"""select r.movieid,r.fresh from rt.reviews as r where r.criticid={0}"""
        for cid in self.criticIds:
            sqlcmd = sqlcmd_.format(cid)
            query = self.trySqlFetchall(sqlcmd)
            mids  = [int(item[0] - 1)   for item in query]
            votes = np.array([float(item[1]) for item in query],dtype='i')
            if list( np.where(votes == self.nullVoteValue))[0]:
                print 'Error: there is vote data that has value == nullVoteValue'
                return

            if getAverageVotes or centerVotes: 
                self.vote_critic_avg[cid-1] = np.mean(votes)
            if centerVotes: 
                votes = votes - self.vote_critic_avg[cid-1]

            thisRow = np.zeros(self.matrix_size[1],dtype='f') + self.nullVoteValue
            thisRow[mids] = votes
            self.vote_matrix[int(cid-1),:] = thisRow
            
        self.vote_matrix_loaded = True
        self.vote_matrix_center = centerVotes


    # User Data
        
    def getMatrixSizeUser(self):
        if not self.userIds.any(): self.getUserIds()
        if not self.movieIds.any(): self.getMovieIds()
        if not self.movieIds.any() or not self.userIds.any():
            print 'Error(getMatrixSize): trouble getting movieIds or userIds'
            return
        else:
            self.matrix_size_user = np.array([np.max(self.userIds),np.max(self.movieIds)],dtype='i')
            return

    def getUserIds(self):
        sqlcmd = u"""select distinct uv.user_id from movies_site.movies_uservotes as uv order by user_id"""
        query = self.trySqlFetchall(sqlcmd)
        if query:
            try:
                self.userIds = np.array([item[0] for item in query],dtype='i')
            except:
                pass

    def allocateVoteMatrixUser(self):
        self.getMatrixSizeUser()
        if not self.matrix_size_user.any():
            print 'Error(allocateVoteMatrixUser): matrix_size_user'
            return
        try:
            self.vote_matrix_user = np.zeros(list(self.matrix_size_user),dtype='f') + self.nullVoteValue
        except:
            pass

    def loadVoteMatrixUser(self,getAverageVotes=True,centerVotes=False):
        if self.debug == True: print 'Call loadVoteMatrixUser'
        if not len(self.vote_matrix_user): self.allocateVoteMatrixUser()
        if not len(self.userIds): self.getUserIds()
        if not len(self.movieIds): self.getMovieIds()
        if not len(self.vote_matrix_user):
            print 'Error(loadVoteMatrixUser): trouble allocating vote_matrix_user'
            return
        if not len(self.userIds) or not len(self.movieIds):
            print 'Error(loadVoteMatrixUser): trouble getting criticIds or movieIds'
            return

        if getAverageVotes or centerVotes:
            self.vote_user_avg = np.zeros(self.matrix_size_user[0],dtype='f') + self.nullVoteValue
            
        sqlcmd_ = u"""select uv.itemId,uv.like from movies_site.movies_uservotes as uv """ + \
                  u"""where uv.itemType=1 and uv.user_id={0}"""        
        for uid in self.userIds:
            sqlcmd = sqlcmd_.format(uid)
            query = self.trySqlFetchall(sqlcmd)
            mids  = [int(item[0] - 1) for item in query]
            votes = np.array([float(item[1]) for item in query],dtype='i')
            if list( np.where(votes == self.nullVoteValue))[0]:
                print 'Error: there is vote data that has value == nullVoteValue'
                return

            if getAverageVotes or centerVotes:
                self.vote_user_avg[uid-1] = np.mean(votes)
            if centerVotes:
                votes = votes - self.vote_user_avg[uid-1]

            thisRow = np.zeros(self.matrix_size_user[1],dtype='f') + self.nullVoteValue
            thisRow[mids] = votes
            self.vote_matrix_user[int(uid-1),:] = thisRow

        self.vote_matrix_user_loaded = True
        self.vote_matrix_user_center = centerVotes


    # user-critic relationship data

    def calculateUserCriticWeightMatrix(self):
        if self.debug ==True: print 'Call calculateUserCriticWeightMatrix'
        if not len(self.vote_matrix): self.loadVoteMatrix()
        if not len(self.vote_matrix_user): self.loadVoteMatrixUser()
        self.weight_matrix = np.zeros([self.matrix_size_user[0],self.matrix_size[0]],dtype='f')
        
        for cid in self.criticIds:
            cRow = self.vote_matrix[cid-1,:]
            cCount = np.array(np.where(cRow != self.nullVoteValue),dtype='i')
            for uid in self.userIds:
                uRow = self.vote_matrix_user[uid-1,:]
                uCount = np.array(np.where(uRow != self.nullVoteValue),dtype='i')
                toCount = np.intersect1d(cCount,uCount)
                #print cid,uid,toCount
                a = cRow[toCount] - np.mean(cRow[cCount])
                b = uRow[toCount] - np.mean(uRow[uCount])
                num = np.dot(a,b)
                den = np.sqrt(np.dot(a,a)*np.dot(b,b))
                if den != 0.0: self.weight_matrix[uid-1,cid-1] = (num/den)

                
    def getOverlappingCriticIds(self):
        if not len(self.criticIds): self.getCriticIds()
        if not len(self.userIds): self.getUserIds()
        
        self.overlappingCriticIds = [ [] for i in range(max(self.userIds))]

        sqlcmd_ = [u"""select r.criticid from movies_site.movies_uservotes as uv """ + \
                   u"""join rt.reviews as r on r.movieid = uv.itemId """ + \
                   u"""where uv.user_id = {0} """ + \
                   u"""group by r.criticid having count(r.movieid) > {1} order by r.criticid asc """,
                   u"""select count(uv.itemId) from movies_site.movies_uservotes as uv """ + \
                   u"""where uv.itemType=1 and uv.user_id = {0}"""]
        
        for uid in self.userIds:

            sqlcmd = sqlcmd_[1].format(uid)
            query = self.trySqlFetchall(sqlcmd)

            nmovies = query[0][0]
            noverlap =max(10,int(np.ceil(0.1*nmovies)))

            sqlcmd = sqlcmd_[0].format(uid,noverlap)
            query = self.trySqlFetchall(sqlcmd)
            overlapList = [item[0] for item in query]

            self.overlappingCriticIds[uid-1] = overlapList


    def initPoolMovieIds(self):
        self.poolMovieIds = [ [] for i in range(max(self.userIds)) ]
            

    def getPoolMovieIds_1User(self,userId):
        if not len(self.overlappingCriticIds): self.getOverlappingCriticIds()
        if not len(self.poolMovieIds): self.initPoolMovieIds()

        sqlcmd_ = u"""select distinct r.movieid from rt.reviews as r where r.criticid = {0}"""
        mids = []
        for cid in self.overlappingCriticIds[userId-1]:
            sqlcmd = sqlcmd_.format(cid)
            query = self.trySqlFetchall(sqlcmd)
            mids = list(set( [item[0] for item in query] + mids))        

            
        sqlcmd_ = u"""select distinct uv.itemId from movies_site.movies_uservotes as uv """ + \
                  u"""where uv.itemType = 1 and uv.user_id = {0}"""
        sqlcmd = sqlcmd_.format(userId)
        query = self.trySqlFetchall(sqlcmd)
        umids = [item[0] for item in query]

        self.poolMovieIds[userId-1] = list(set(mids) - set(umids))


    def getPoolMovieIds(self):  
        if not len(self.userIds): self.getUserIds()
        for uid in self.userIds:
            self.getPoolMovieIds_1User(userId=uid)


    def calculatePredictedMovieVotes(self,minCritics=10):
        if not len(self.weight_matrix): self.calculateUserCriticWeightMatrix()
        if not len(self.overlappingCriticIds): self.getOverlappingCriticIds()
        if not len(self.poolMovieIds): self.getPoolMovieIds()
        
        self.predictedMovieVotes = [ [] for item in range(max(self.userIds)) ]
        
        for uid in self.userIds:

            cids = np.array([ cid - 1 for cid in self.overlappingCriticIds[uid-1] ],dtype='i')
            movieIndex = self.poolMovieIds[uid-1]
            abs_wjk = np.abs(self.weight_matrix[uid-1,:])

            wrow = self.weight_matrix[uid-1,:]
            wrow = wrow[cids]
            avgUserVote = self.vote_user_avg[uid-1]

            predictedVotes = []

            for mid in self.poolMovieIds[uid-1]:

                crow = self.vote_matrix[:,mid-1]
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


            self.predictedMovieVotes[uid-1] = predictedVotes
