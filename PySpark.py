
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession     .builder     .appName("Python Spark SQL basic example")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[3]:


df = spark.read.json("/people.json")
# Displays the content of the DataFrame to stdout
df.show()


# In[4]:


import pandas as pd

data = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('2008.csv')
#pd=spark.read.csv("2008.csv", header='None')

data.first()



# In[5]:


pd=data
# spark, df are from the previous example
# Print the schema in a tree format
pd.printSchema()
pd.dtypes



# In[7]:


#pd.summary()


# In[5]:


#pd.dtypes
pd.describe("WeatherDelay").show()

#pd.stat.approxQuantile("Year",(0.25,0.5,0.75),0.0)


# In[9]:


pd.groupBy("Cancelled").count().show()
pd.groupBy("Origin").count().show()

pd.groupBy("SecurityDelay").count().show()

#pd.select("Cancelled").show()

#pd.select(pd['Year'], pd['Month'] + 1).show()
# +-------+---------+

# Select people older than 21
#pd.filter(pd['Distance'] > 21).show()
# Count people by age


# In[13]:


pd.head()


# In[9]:


from __future__ import division, print_function
from matplotlib import pyplot as plt
# In a notebook environment, display the plots inline
get_ipython().magic(u'matplotlib inline')

# Set some parameters to apply to all plots. These can be overridden
# in each plot if desired
import matplotlib
# Plot size to 14" x 7"
matplotlib.rc('figure', figsize = (14, 7))
# Font size to 14
matplotlib.rc('font', size = 14)
# Do not display top and right frame lines
matplotlib.rc('axes.spines', top = False, right = False)
# Remove grid lines
matplotlib.rc('axes', grid = False)
# Set backgound color to white
matplotlib.rc('axes', facecolor = 'white')


# In[ ]:


# Define a function to create the scatterplot. This makes it easy to
# reuse code within and across notebooks

def scatterplot(x_data, y_data, x_label, y_label, title):

    # Create the plot object
    _, ax = plt.subplots()

    # Plot the data, set the size (s), color and transparency (alpha)
    # of the points
    ax.scatter(x_data, y_data, s = 30, color = '#539caf', alpha = 0.75)

    # Label the axes and provide a title
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

# Call the function to create plot
scatterplot(x_data = pd['FlightNum']
            , y_data = pd['Cancelled']
            , x_label = 'Flight Number'
            , y_label = 'Cancelled'
            , title = 'Flights vs Cancelled')

