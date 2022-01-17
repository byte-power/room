How to use:

Send contents of these files to redis server and room.
Compare the difference of the response

```
nc 127.0.0.1 6379 -v  < tx_failed_pipeline.resp
```

```
cat tx_failed_pipeline.resp | nc 127.0.0.1 6379 -v
```


