from mrjob.job import MRJob

class NumOfRatings(MRJob):

        # Mapper Function
        def mapper(self, _, line):
        	#print(line.split('\t'))
                (userID,movieID, rating, timestamp) = line.split(',')
                yield(rating, 1)
                        
        # Reducer Function
        def reducer(self, rate, counts):
                yield(rate, sum(counts))

if __name__ == '__main__':
	NumOfRatings.run()
