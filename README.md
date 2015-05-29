# Barbante Recommender Engine

Barbante is meant to be a configurable, all-purpose recomendation machine. Based on the history of activities of users and on the attributes of items (or *products*), Barbante combines some recommendation strategies and returns the top-ranked available items.

A REST interface is provided, so the client system can trigger the tracking of its users' activities. The request for recommendations can be done via REST interface as well. A concept that is very important for the qualitiy of recommendations providade by Barbante algorithms is that of *impressions*. A product has an impression on a certain user if it has ever been shown to that user. An endpoint to keep track of impressions is also available in the REST interface.

The current implementation of Barbante uses mongodb to handle storage and access to data. The barbante/data/MongoDBProxy.py file implements the interface in barbante/data/BaseProxy.py. Other database solutions may be used at will, provided corresponding implementations of the BaseProxy interface are created.

## Customization

The customization of Barbante to meet the specific needs of a certain client is done via two configuration files:
  
  - barbante/config/mongoid.yml
  - barbante/config/barbante\_&lt;client_name&gt;.yml

In the barbante/config/mongoid.yml given here as an example, we have added entries for two client systems, good old "foo" and "bar". The examples in the file are quite self-explanatory, but one thing is vital: the value assigned to the "customer" attribute should be the same as &lt;client_name&gt;.

For instance, in the given mongoid.yml, we have

    foo:
      customer: Foo
      sessions:
        default:
          database: db_foo
          hosts:
            - host_foo.mydomain.com:27017
          database_raw: db_foo_raw
          hosts_raw:
            - host_foo_raw.mydomain.com:27017
          options:
            read: primary
            max_retries: 1
            retry_interval: 0
            pool_size: 35
      options:
        raise_not_found_error: False

The client system should therefore use the alias "foo" to identify itself when making REST requests, and the configuration file that will be used by Barbante to provide recommendations for that specific client will be barbante/config/barbante_Foo.yml.

The file barbante_Foo.yml that is provided has a number of parameters that can be fine-tuned to attain better recomendations. Each one of them is very briefly explained in the file itself. The default values that are given should work out fine, anyway, for the great majority of cases.

However, there are three sections in said file that *must* be customized: 

  - the *ACTIVITIES* section, where each user activity that gets tracked by the client is assigned a number of stars (in the usual 5-star rating system), among some other minor configurations;
  - the *PRODUCT_MODEL* section, where each attribute of the product is classified (as text, numeric, list etc.) and assigned a weight with respect to content-based similarity; and
  - the *ALGORITHM_WEIGHTS* section, where each recommender algorithm is assigned a weight, i.e., a measure of its importance in the overall ranking of products.

It should be fairly simple to do that based on the existing examples. We remark that the 5-star rating correspondence assigned to each activity is used to obtain "implicit" user-to-product ratings in systems which do not explicitly obtain such ratings. Note that, in the case of client systems which do collect explicit ratings, such ratings can be tracked as normal activities all the same, e.g., activity "rate-5-stars" corresponding to a rating of 5, "rate-4-stars" corresponding to a rating of 4, etc.


## REST interface

The list below shows the main endpoints which are currently available, and their GET/POST parameters: 

  - POST
    - process_activity_fastlane
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - user_id: the unique identifier of the user;
      - product_id: the unique identifier of the product;
      - activity\_type: the type of the activity, which should be one of the types configured in barbante/config/barbante_&lt;client_name&gt;.yml file;
      - activity\_date: the date/time of the activity, in ISO format

    - process_activity_slowlane
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - user_id: the unique identifier of the user;
      - product_id: the unique identifier of the product;
      - activity\_type: the type of the activity, which should be one of the types configured in barbante/config/barbante_&lt;client_name&gt;.yml file;
      - activity_date: the date/time of the activity, in ISO format
      
    - process_product
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - product: a JSON document with all relevant product attributes, as configured in barbante/config/barbante\_&lt;client_name&gt;.yml file;

   - process_impression
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - user_id: the unique identifier of the user;
      - product_id: the unique identifier of the product;
      - impression_date: the date/time of the activity, in ISO format

    - delete_product
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - product_id: the unique identifier of the product

  - GET
    - recommend/&lt;env&gt;/&lt;user\_id&gt;/&lt;count\_recommendations&gt;/&lt;algorithm&gt;/&lt;context\_filter&gt;
      - env: the "environment", i.e., the alias for the client in barbante/config/mongodb.yml file;
      - user_id: the unique identifier of the intended target user;
      - count_recommendations: the desired number of recommendations;
      - algorithm: the identification of the algorithm (UBCF, PBCF, CB, POP, HRChunks, HRRandom, HRVoting);
      - context\_filter: an optional JSON document with product attributes to be matched (the supported product attributes are those marked "context\_filter: true" in the PRODUCT\_MODELS section in barbante/config/barbante\_&lt;client_name&gt;.yml file)


## MongoDB

Other than setting up a mongodb instance and pointing to it accordingly in barbante/config/mongodb.yml file, one should execute the Python script barbante/scripts/ensure\_all\_idexes.py. It will create all (initially empty) collections and indexes required by Barbante.


## Recommender algorithms

There are currently four recommender algorithms (or "specialists"), and three merge strategies (or "hybrid algorithms") available.

Specialists:

  - UBCF (User-Based Collaborative Filtering)
  - PBCF (Product-Based Collaborative Filtering)
  - CB (Content-Based)
  - POP (Popularity)

  
Hybrid algorithms:

  - HRChunks
  - HRRandom
  - HRVoting


### UBCF

Roughly speaking, the user-based collaborative filtering ranks items based on previous activities (or "consumption patterns") of like-minded users. When recommending products for a given *target user* Alice, the UBCF specialist first gathers the *COUNT_USER_TEMPLATES* users whose recently viewed (or bought, or read, etc., according to the client settings) products maximize the likelihood of being interesting to Alice. Such like-minded users are refered to as *user templates*. The most recent 5-star-rated products of each user template are then scored according to the "degree of templateness" (or simply the *strength*) of each user template with respect to Alice. The number of user templates to be considered corresponds to an entry in barbante/config/barbante\_&lt;client_name&gt;.yml file. The strength of each user *U* in the system as a user template for Alice is obtained as the probability (inferred by observing historical data of both *U* and Alice) that products rated high by *U* (and with impressions to Alice) are also rated high by Alice. 


### PBCF

The Product-Based collaborative filtering works as follows. Each of the *COUNT_RECENT_PRODUCTS* most recently consumed (implicitly rated "high enough") products of our target user Alice is regarded as a *base product*. For each base product, the algorithm gathers the *COUNT_PRODUCT_TEMPLATES* products which maximize the probabilities of being interesting to other users who rated high said base product. Such probabilities are inferred based on historical data of *all* users in the client system. As expected, the number of base products and of templates of base products are configured as entries in barbante/config/barbante\_&lt;client\_name&gt;.yml file.

### CB

The Content-Based specialist works to some extent analogously to the PBCF, for it also retrieves a number of base products of the target user. Only now the template products of each base product will be those which are most intrinsically similar to the base product. The degree of similarity is obtained by the weighted comparison of each relevant attribute. Text attributes are compared using an original algorithm (based on "asymmetric debts") to be published soon.


### POP

The simplest specialist is the Popularity recommender, which ranks items based on their "popularity density", that is, based on how many users have rated each item high enough divided by the time span (in days) during which the activities on that item have occurred.


### HRChunks

The first hybrid recommender merges the four specialists above in such a way that each specialist's items occupy a fixed chunk (number of slots) in the overall ranking of items. These chunks are circularly repeated until the items of all specialists have been exhausted (or the intended number of recommendations has been reached). The size of each chunk is defined according to the weights assigned to each specialist in the ALGORITHM\_WEIGHTS section in barbante/config/barbante\_&lt;client_name&gt;.yml file.


### HRRandom

The second hybrid recommender merges the four specialists in random fashion. For each "product slot" in the overall ranking being built, it chooses randomly the topmost item of one of the specialists, removing it from that specialist's queue. The probability to choose each of the specialists conforms to the weight assigned to it in the ALGORITHM_WEIGHTS section in barbante/config/barbante\_&lt;client_name&gt;.yml file.

### HRVoting

The third hybrid recommender --- which seems to be the one with most interesting results --- employs a voting system to combine the rankings of all specialists. The ranking produced by each specialist is first used to assign scores for each product in a Formula 1 fashion: the first item gets a number of points, the runner-up gets less points, and so on. A negative exponential function is applied to determine the number of points each position in the ranking deserves. The points given to a same item by more than one specialist are summed up, but not until they have been multiplied by the appropriate specialist weight, as configured in the ALGORITHM\_WEIGHTS section in barbante/config/barbante\_&lt;client_name&gt;.yml file.



