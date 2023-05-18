# Edit settings
Edit the <strong>settting.py</strong> to edit the entity sets per second in a iteration, increase of entity sets per second for every iteration, the cutoff point of the test program, sizes of the entities, logging file paths and formats, the subscriber timout if no data is tranfers. There is also an option to get results printed to stdout.

# Run the single topic test
<strong>
python ./middleware.py <br>
python ./single_topic_subscriber.py <br>
python ./single_topic_publisher.py
</strong>

# Run the sub topic test
<strong>
python ./middleware.py <br>
python ./sub_topic_subscriber.py <br>
python ./sub_topic_publisher.py
</strong>

# KEEP IN MIND
ALWAYS restart the middleware.py as it cache data. Otherwise the test will be corrupt.

# In relation to the study
* Mean latency in the related study is the same as average latency in this code
* The results shown in the related study can be found in this git repo under the *results* folder
