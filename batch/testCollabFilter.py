#!/usr/bin/python

"""
Crude Test and Timing
"""

from collabFilterFunc import *
import time
import numpy as np

def timeit(f):
    def inner(*args,**kwargs):
        print '{0}'.format(f.__name__)
        tstart = time.time()
        ret = f(*args,**kwargs)
        trun = time.time() - tstart
        print 'runtime (sec) = {0}'.format(trun)
        return ret
    return inner


connectMysql = timeit(connectMysql)
con = connectMysql(db='movies_site',dbuser='',dbpw='')

getCriticIds = timeit(getCriticIds)
criticIds = getCriticIds(con)

getMovieIds = timeit(getMovieIds)
movieIds  = getMovieIds(con)

getUserIds = timeit(getUserIds)
userIds = getUserIds(con)

getVoteMatrixCritic = timeit(getVoteMatrixCritic)
vote_matrix_critic = getVoteMatrixCritic(con,criticIds,movieIds)

getVoteMatrixUser = timeit(getVoteMatrixUser)
vote_matrix_user = getVoteMatrixUser(con,userIds,movieIds)

uid = 2

getVoteVectorUser = timeit(getVoteVectorUser)
vote_vector_user = getVoteVectorUser(con,uid,movieIds)

calculateWeightVectorUser = timeit(calculateWeightVectorUser)
weight_vector = calculateWeightVectorUser(uid,vote_vector_user,vote_matrix_critic,criticIds)

getOverlappingCriticIds = timeit(getOverlappingCriticIds)
overlappingCriticIds = getOverlappingCriticIds(con,uid,criticIds,noverlap_min=10)

getPoolMovieIds = timeit(getPoolMovieIds)
poolMovieIds = getPoolMovieIds(con,uid,overlappingCriticIds)

calculatePredictedMovieVotes = timeit(calculatePredictedMovieVotes)
predictedMovieVotes = calculatePredictedMovieVotes(uid,weight_vector,vote_vector_user,vote_matrix_critic,overlappingCriticIds,poolMovieIds)
