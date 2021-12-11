box.cfg()

http_client = require('http.client').new({max_connections = 5000})

json = require('json')

mqtt = require('mqtt')

digest = require('digest')

-- Create instance
connection = mqtt.new()

connection:login_set('Hans', 'Test')

-- Connect to the server
connection:connect({host='194.67.112.161', port=1883})

options = table.deepcopy({['headers']={['content-type'] = 'application/vnd.kafka.binary.v2+json'}})

partsize = 5000;
counter = 0;
body = ''

-- Set callback for recv new messages
connection:on_message(function (message_id, topic, payload, gos, retain)
    local newdata = json.decode(payload)
    --print(digest.base64_encode(json.encode({day = 12, ticktime = 1.2, speed = 5.3}, { __serialize="map"})))
    --resp = http_client:post('http://192.168.1.70:8082/topics/task9_11','{"records": [{"value": "'..digest.base64_encode(json.encode({day = newdata.Day, ticktime = newdata.TickTime, speed = newdata.Speed}, { __serialize="map"}))..'"}]}',options)
    body = body .. json.encode({day = newdata.Day, ticktime = newdata.TickTime, speed = newdata.Speed}, { __serialize="map"})
    counter = counter +1
    if counter > partsize then
        resp = http_client:post('http://192.168.1.70:8082/topics/task9_14','{"records": [{"value": "'..digest.base64_encode(body,{nowrap=true})..'"}]}',options)
        body = ''
        counter = 0
        print(resp.body)
    end
end)

-- Subscribe to a system topic
connection:subscribe('v14')