# ReadMe

## How to use

Send contents of these files to redis server and room.
Compare the difference of the response

```bash
nc 127.0.0.1 6379 -v  < tx_failed_pipeline.resp
```

```bash
cat tx_failed_pipeline.resp | nc 127.0.0.1 6379 -v
```

## commands in files

### pipeline.resp

```
set {a}1 a1
set {b}2 abc
get {a}1
```

### pipeline_with_error.resp

```
set {a}1 a1
set {b}2 abc xyz
get {a}1
```

### tx_pipeline.resp

```
watch {b}2
set {a}1 a1
set {b}1 b1
set {b}4 b4
multi 
set {b}2 abc
set {b}3 mn
exec
get {a}1
get {b}2
```

### tx_watch_failed_pipeline.resp

```
watch {b}2
set {a}1 a1
set {b}2 b2
set {b}3 b3
multi 
set {b}2 abc
set {b}3 mn
exec
get {a}1
get {b}2
```

### tx_discard_pipeline.resp

```
watch {b}2
set {a}1 a1
multi
set {b2} abc
set {b3} mn
discard
get {a}1
get {b}2
```

### tx_failed_pipeline.resp

```
watch {b}2
set {a}1 a1
multi
set {b}2 abc
set {b}4 ab abc
set {b}3 mn
exec
get {a}1
get {b}2
```

### tx_cross_slot_pipeline.resp

```
watch {b}2
set {a}1 a1
multi
set {b}2 abc
set {a}3 mn
set {b}3 mn
exec
get {a}1
get {b}2
```
