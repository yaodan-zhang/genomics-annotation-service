# gas-framework

An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:

* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

---

## The archive process

After an annotation job is completed, if the user is a free user, the annotator will send a message to a job archive queue (realized by Amazon SQS). The archive process (realized in `util/archive/`) receives the message and archives the result file (not the log file) to Glacier.

## The restore process

If a user upgrades to premium, the web server will send a message to a restore queue (realized by Amazon SQS). The restore process (implemented in `util\restore`) receives the message from the queue and submits an expedited retrieval request to restore all archived files of the user from Glacier. If the expedited retrieval request fails, standard retrieval is used. After the retrieval process is completed, a notification is sent to an Amazon SNS topic, the thaw queue (realized by Amazon SQS) subscribing the topic then receives a message from the topic. Then, `util\thaw` receives the message from the thaw queue, downloads the retrieved files from Glacier, and uploads them to S3 for the user to download on the GAS web page.

## Remarks

1. The references are listed on the top of each file if there are any, instead of listed as inline comments as specified in the project description. Hope this still works (the purpose is for asthentic improval).
2. If you are testing the autoscaling group and terminate all instances, after new instances are initialized, it takes some time for them to pass the health checks as you might first see they appear to be unhealthy, and then healthy after a couple of minutes (5-10minutes). Please be patient.
