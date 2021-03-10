# streamlit_callbacks

The interface delay times are always in seconds.

Basic usages
---

**Callback later:**

example of rerun:
```
from streamlit.callbacks.callbacks import later, rerun
import streamlit as st
from datetime import datetime

st.write(datetime.now())

later(2.0, rerun) # After 2 sec calls a rerun
```

**Periodically:**
```
from streamlit.callbacks.callbacks import periodic
import streamlit as st

st.button('rerun')
em = st.empty()

sec = -1
def call():
  global sec
  sec += 1
  em.write(f'~{sec} sec without rerun')

call()

periodic(1.0, call)
```

rerun/stop periodic callback
```
from streamlit.callbacks.callbacks import periodic
import streamlit as st

em = st.empty()

sec = -1
def call():
  global sec
  sec += 1
  em.write(f'~{sec} sec without rerun')
  if sec > 10:
    st.experimental_rerun()
    # to stop periodic call, simply call st.stop()

call()

periodic(1.0, call)
```


Websocket
---

**Websocket client connection (receive/send):**
```
import streamlit.callbacks.websocket as ws
import streamlit as st

ws_conn = 'ws://localhost:10000'

print_message = st.empty()
print_message.write("websocket message placeholder")

def msg_handler(msg):
  print_message.write(f"Arrived message from ws: {msg}")
  ws.send_message(ws_conn, 'Thanks for the message!')
  
ws.on_message(ws_conn, msg_handler)
```

**Websocket client buffered message:**

Sometimes when websocket send messages frequently, it need to handled buffered way, with accepted some delay

```
import streamlit.callbacks.websocket as ws
import streamlit as st

ws_conn = 'ws://localhost:10000'

print_message = st.empty()
print_message.write("websocket message placeholder")

def msg_handler(msgs):
  endline = '    \n -> '
  print_message.write(f"Arrived messages from ws:{endline}{endline.join(msgs)}")
  ws.send_message(ws_conn, 'Thanks for the messages!')
  
ws.on_message_buffered(ws_conn, msg_handler, buffer_time=0.5)
```

Redis
---

**Redis sub-pub channels**

basic, with one channel
```
import streamlit.callbacks.redis as redis
import streamlit as st

print_message = st.empty()
print_message.write("Redis channel message placeholder")

def msg_handler(channel, message):
  print_message.write(f"Arrived message from channel '{channel}': {message}")
  
redis.on_message('cat', msg_handler)
```

multiple channel with regex (only '.?' and '.*' supported, and transformed as redis psubscribe '?' and '\*')
```
import streamlit.callbacks.redis as redis
import streamlit as st
import re

print_message = st.empty()
print_message.write("Redis channel message placeholder")

def msg_handler(channel, message):
  print_message.write(f"Arrived message from channel '{channel}': {message}")
  
redis.on_message(re.compile('.*'), msg_handler)
```

**Redis pub-sub buffered messages and publish (send):**

```
import streamlit.callbacks.redis as redis
import streamlit as st
import re

redis_addr = ('localhost', 6379)

print_message = st.empty()
print_message.write("Redis channel message placeholder")

def msg_handler(msgs):
  endline = '    \n'
  to_print = f'Arrived messages from redis:{endline}'
  for channel, message in msgs:
    to_print += f'"{channel}" -> {message}{endline}'
  print_message.write(to_print)
  redis.send_message('out_streamlit', 'Thanks for the messages!')
  
redis.on_message_buffered(re.compile('in_.*'), msg_handler, buffer_time=0.5)
```
