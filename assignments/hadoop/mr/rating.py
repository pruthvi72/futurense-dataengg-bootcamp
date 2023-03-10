from mrjob.job import MRJob


class Rating_count(MRJob):

        """ The below mapper() function defines the mapper for MapReduce and takes
        key value argument and generates the output in tuple format .
        The mapper below is splitting the line and generating a word with its own
        count i.e. 1 """
        def mapper(self, _, line):
        	#print(line.split('\t'))
                (userID,movieID, rating, timestamp) = line.split(',')
                yield(rating, 1)
                        
        """ The below reducer() is aggregating the result according to their key and
        producing the output in a key-value format with its total count"""
        def reducer(self, rate, counts):
                yield(rate, sum(counts))

"""the below 2 lines are ensuring the execution of mrjob, the program will not
execute without them"""
if __name__ == '__main__':
	Rating_count.run()