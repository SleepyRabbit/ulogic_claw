const Log = require('log'),
    log = new Log();
const Promise = require("bluebird");
const deepstream = require('deepstream.io-client-js')
const Validator = require('validatorjs');
const config = require('./app.config.json');

const CONST = {
    queue_occupation_max: 3, //同一个人在队伍中的最多占位
    concurrent_player_max: 1, //游戏同时操作的人数
    ticket_null: ""
}

const CONST_uLOGIC_NAME = config.uLogicClaw.uLogicName;
const CONST_iDS_LIST_NAME = config.uLogicClaw.iDSListName;
const CONST_uLOGIC_SERVER = config.uLogicClaw.dsURL;
const CONST_MAX_QUEUE_LENGTH = config.uLogicClaw.MaxQueueLength;

const glb_catcher_ids = [9527]; //在册设备编号
const glb_catcher_records = []; //record 集合

function invalids_stringify(errors) {
    var info = "";
    for (var seg in errors)
        info += `${seg}:${errors[seg][0]}.`

    return info;
}

var client = deepstream(CONST_uLOGIC_SERVER).login({
    username: 'ulogic.claw',
    password: 'thisisatestkey' // NEEDS TO BE REAL
}, function(success, data) {

    if (!success) {
        log.error("Login error: " + data)
    } else {
        var is_reconnect = glb_catcher_records[glb_catcher_ids[0]] != undefined;

        //console.log("record length:" + glb_catcher_records.length);

        if(is_reconnect){
            log.info("Relogin success!! deinit records and rpc first.");

            //全部清空
            for(var idx in glb_catcher_records){
                if(glb_catcher_records[idx])
                    glb_catcher_records[idx].discard();
                delete glb_catcher_records[idx];
            }
            
            client.rpc.unprovide('rpc/${CONST_uLOGIC_NAME}/participate');
        }
        else{
            log.info("Login success!!!!");

            client.on('error', function(error, event, topic) {
                log.warning("[Client 'error' evt]:" + error, event, topic);
            })

            client.on('connectionStateChanged', function(connectionState) {
                log.info("[State evt]connStateChanged: " + connectionState);
            });

        }

        // 从deepstream读取设备列表list
        let record_list_ids = client.record.getList(CONST_iDS_LIST_NAME);   // getList

        record_list_ids.whenReady( (list) => {
            "use strict";
            if( list.isEmpty() ) {
                log.info("You don't have any entry!");
            }
            else {
                let entries = list.getEntries();                     // getEntries
                entries.forEach( (entry, index) => {                 // Entries数组中存放的是字符串，而glb_catcher_ids是数值型数组，需要进行转换。
                    glb_catcher_ids[index] = parseInt(entry);
                } );
                log.info("Have got list entries from deepstream!");
            }

            //准备一堆 record
            for (var i in glb_catcher_ids)
                catcher_record_prepare(client, glb_catcher_ids[i]);
            log.info("Records prepared.");

            client.rpc.provide(`rpc/${CONST_uLOGIC_NAME}/participate`, rpc_uLogic_claw_participate);
            log.info("RPC prepared.");

            //  订阅并处理来自设备侧的消息，设备将通过event的形式与ulogic模块进行通信，消息的格式为evt/scene/**
            client.event.subscribe('evt/scene/' + CONST_uLOGIC_NAME, evt_catcher_scene );
            log.info("Has subscribed: evt/scene/" +  CONST_uLOGIC_NAME);

            log.info(glb_catcher_ids);

        });


        //定时更新设备状态
        //todo

        //优雅退出
    }
});



function evt_catcher_scene(data) {

    // data {
    //   sceneDID: 9527,
    //   state: 'idle',
    // }

    if(!data.sceneDID || !data.state) {
        log.info('Wrong evt_catcher_scene data:!');
        return;
    }

    log.info("evt_catcher_scene called!");

    switch ( data.state ) {
        case 'ready':
            log.info("evt idle");
            //判断设备的sceneDID是否已经在glb_catcher_ids中
            log.info(glb_catcher_ids);
            if( glb_catcher_ids.indexOf(data.sceneDID) <0 ) {
                log.info("New device: " + data.sceneDID);
                glb_catcher_ids.push(data.sceneDID);

                // 如果设备DID不在record list entries的记录中，需要添加进去
                record_catcher_ids = client.record.getList(CONST_iDS_LIST_NAME);   // getList
                record_catcher_ids.whenReady( (list) => {
                    "use strict";
                list.addEntry(String(data.sceneDID));

                let entries = list.getEntries();
                log.info(entries);
                 } )
            }
            else {
                log.info(glb_catcher_ids);
                log.info("Device did already stored in glb_catcher_ids!");

                const str_record_catcher = `cache/${CONST_uLOGIC_NAME}/${glb_catcher_ids}`;
                const str_event_catcher = `event/${CONST_uLOGIC_NAME}/${glb_catcher_ids}`;

                var record = client.record.getRecord(str_record_catcher);

                record.whenReady(() => {
                    "use strict";

                })
            }

            // 
           break;
        // case '':
        //     break;
        default:
    }
}


//为某一个编号的catcher准备接口和
function catcher_record_prepare(client, catcherID) {
    const str_record_catcher = `cache/${CONST_uLOGIC_NAME}/${catcherID}`;
    const str_event_catcher = `evt/${CONST_uLOGIC_NAME}/${catcherID}`;
    console.log(str_record_catcher);

    var record = client.record.getRecord(str_record_catcher);

    record.whenReady(() => {

        /* record的基本结构
        {
        	device: "online/play/offline"
        	queue:[],	//队列
        	stream:"rtmp://xx"	//流地址
        }
        */
        // record.delete();
        let r = record.get();
        if (!r.state || !r.stream || !r.q_usr) {
            if (!r.state)
                r.state = catcher_camera_reset(catcherID);
            if (!r.stream)
                r.stream = catcher_stream_reset(catcherID);
            if (!r.q_usr)
                r.q_usr = catcher_queue_reset(catcherID);
            if (!r.q_screen)
                r.q_screen = catcher_qscreen_reset(catcherID);
            if (!r.params)
                r.params = catcher_params_info(catcherID);

            record.set(r);
            log.info(`Init ds_record: ${str_record_catcher}`);
        }

        //登记
        glb_catcher_records[catcherID] = record;

    });

    record.on('error', (error)=> {
            log.warning("[Record 'error' evt]:" + error);
    });



}

/*
 参与队列，参数如下：
 {
 catcherID: number,
 user_id: string,
 }
*/

function rpc_uLogic_claw_participate(data, response) {
    "use strict";
    log.info('rpc_uLogic_claw_participate called!');
    //response.autoAck = false;
    var valid_param = new Validator(data, {
        catcherID: 'required|integer',
        userID: 'required|string',
    });

    if (valid_param.fails()) {	//tested
        var err = "param error" + invalids_stringify(valid_param.errors.all())
        log.warning("rpc_catcher_participate: " + err);
        return response.error(err);
    }

    const str_record_catcher = `cache/${CONST_uLOGIC_NAME}/${data.catcherID}`;
    const str_event_catcher = `event/${CONST_uLOGIC_NAME}/${data.catcherID}`;


    //检查glb_catcher_records中是否记录了当前设备ID所对应的record
    if (glb_catcher_records[data.catcherID] === undefined) {	//tested
        const err = `${data.catcherID} not find.`;
        log.warning("rpc_catcher_participate: " + err);
        return response.error(err);
    }

    //提取catcher状态
    /*
     snapshot获取数据结构：
     {
         state:{status:"online/offline/error"},
         stream:"rtmp://xx"
         //参与者排队id数组
         q_usr:[“aa”,"bb","cc"],
         q_screen:["zH32sD", "", "..."], //空串表示未绑定屏
         params:{
         queue: {rest:3, driver:1, wait:10, free:2}   //队列信息
         }
     }
     */

    log.info(str_record_catcher);

    client.record.has(str_record_catcher, (err, hasRecord) => {
        if(hasRecord) {
            client.record.snapshot(str_record_catcher, (err, dataRecord) => {
                // console.log(dataRecord);
                if(!dataRecord.q_usr) {
                    log.info("No queue data!");
                }
                else {
                    console.log(dataRecord.q_usr);
                    console.log(data.userID);
                    console.log(dataRecord.q_usr.indexOf(data.usrID));
                    if( dataRecord.q_usr.indexOf(data.usrID) <0 ) {
                        let q_len = dataRecord.q_usr.length;   //获取排队数组长度
                        if(q_len < CONST_MAX_QUEUE_LENGTH) {
                            dataRecord.q_usr.push(data.userID);
                            glb_catcher_records[data.catcherID].set(dataRecord);
                            log.info("Join the queue...")
                        }
                        else {
                            log.info("The queue is full!");
                        }
                    }
                    else {
                        log.info("You are already in the queue!");
                    }
                }
            })
        }
    })

}

/*
	参与队列，参数如下：
	{
		catcherID: number,
		user_id: string,
		ticket: string
	}

*/
function rpc_catcher_enroll(data, response) {

    //response.autoAck = false;
    var valid_param = new Validator(data, {
        catcherID: 'required|integer',
        user_id: 'required|string',
        ticket: 'string'
    });


    if (valid_param.fails()) {	//tested
        var err = "param error" + invalids_stringify(valid_param.errors.all())
        log.warning("rpc_catcher_participate: " + err);
        return response.error(err);
    }



    const str_record_catcher = `cache/${CONST_uLOGIC_NAME}/${data.catcherID}`,
    str_event_catcher = `event/${CONST_uLOGIC_NAME}/${data.catcherID}`;

    //没有找到catcher
    if (glb_catcher_records[data.catcherID] === undefined) {	//tested
        const err = `${data.catcherID} not find.`;
        log.warning("rpc_catcher_participate: " + err);
        return response.error(err);
    }


    data.ticket = data.ticket || CONST.ticket_null;    //归拢到空字串 ""


    //票据检查
    catcher_ticket_handle(client, data.user_id, data.ticket).then(
        //没有票据 或者 处理通过
        () => {

            //console.log(data.catcherID);
            //console.log(glb_catcher_records[data.catcherID]);

            //提取catcher状态
            var catcher_record = glb_catcher_records[data.catcherID];
            var r = catcher_record.get();

            //console.log(JSON.stringify(r, null, 2));

            //设备不在线
            if (r.camera.status != "online") {
                const err = `game ${data.catcherID} camera ${r.camera.status}.`;
                log.warning("rpc_catcher_participate: " + err);
                return response.send({ message: err });
            }

            //获取队列中所有的位置
            const pos = utl_queue_pos(data.user_id, r.queue);




            //还有玩家位
            if(r.queue.length < CONST.concurrent_player_max){
                r.queue.push(data.user_id);
                r.qticket.push(data.ticket);

                catcher_record.set(r);  //直接加入队列并记录
                client.event.emit(str_event_catcher, str_record_catcher);   //向event channel 发送 record 消息

                const err = utl_who_do_thing_with_what(data.user_id, "play", data.catcherID, data.ticket?data.ticket:"melon");
                log.info("rpc_catcher_participate: " + err);
                return response.send({ record: str_record_catcher, event: str_event_catcher });
            }
            //玩家位被占，但是队伍还有空
            else if (r.queue.length < r.game.q_capacity) {

                if (!data.ticket){   //携瓜入场
                 
                    //是否已经排在队里
                    if (pos.length > CONST.queue_occupation_max) {
                        //占位太多就不太好了，让别人玩玩嘛
                    } else {
                        //尾部占个位
                        r.queue.push(data.user_id);
                        r.qticket.push(data.ticket);
                        catcher_record.set(r);
                        client.event.emit(str_event_catcher, str_record_catcher);   //向event channel 发送 record 消息

                        const err = utl_who_do_thing_with_what(data.user_id, "join", data.catcherID, "melon");
                        log.info("rpc_catcher_participate: " + err);
                    }
                    return response.send({ record: str_record_catcher, event: str_event_catcher });
                }

                else{   //携票入场
                    var p = r.qticket.indexOf(CONST.ticket_null, CONST.concurrent_player_max); //从玩家之后 找到票为0的位置
                    if(p < 0) p = r.queue.length;   //找不到瓜众 就排在后面呗
                    r.queue.splice(p, 0, data.user_id); //插队在最后一个持票者之后
                    r.qticket.splice(p, 0, data.ticket);

                    catcher_record.set(r);
                    client.event.emit(str_event_catcher, str_record_catcher);   //向event channel 发送 record 消息

                    const err = utl_who_do_thing_with_what(data.user_id, "join", data.catcherID, data.ticket?data.ticket:"melon");
                    log.info("rpc_catcher_participate: " + err);
                    return response.send({ record: str_record_catcher, event: str_event_catcher });

                }

            }
            //没位
            else {
                var p0 = r.qticket.indexOf(CONST.ticket_null, CONST.concurrent_player_max); //从玩家之后 找到票为0的位置

                //大爷持票入场，吃瓜观众可以让让啦
                if (p0 > -1 && data.ticket) {
                    //数据告诉我们，只要存在吃瓜观众，最后一个必定拿瓜

                    const victim = r.queue.pop();
                    r.qticket.pop(); //踢掉最后一个

                    r.queue.splice(p0, 0, data.user_id); //插队在最后一个持票者之后
                    r.qticket.splice(p0, 0, data.ticket);

                    catcher_record.set(r);
                    client.event.emit(str_event_catcher, str_record_catcher);   //向event channel 发送 record 消息

                    const err = utl_who_do_thing_with_what(data.user_id, "jump into", data.catcherID, data.ticket?data.ticket:"melon", `kick out user'${victim}'`);
//                    const err = `User"${data.user_id}" jump into game"${data.catcherID}" with ticket "${data.ticket}", kick out user"${victim}".`;
                    log.info("rpc_catcher_participate: " + err);

                    return response.send({ record: str_record_catcher, event: str_event_catcher });
                }

                //有票没位 或者 没票，反正都没法排上了
                else {

                    //你已经在队伍里，也许你只是掉线刚回来
                    if (pos.length > 0) {
                        const err = utl_who_do_thing_with_what(data.user_id, "back to", data.catcherID, data.ticket?data.ticket:"melon");
                        //const err = `User"${data.user_id}" come back to game"${data.catcherID}".`;
                        log.info("rpc_catcher_participate: " + err);

                        return response.send({ record: str_record_catcher, event: str_event_catcher });
                    }

                    //实在没招了。返回一个人数统计：{full: capacity/player/ticket/melon}
                    else {
                        var tickets, melon, p0 = r.qticket.indexOf(CONST.ticket_null, CONST.concurrent_player_max); //从玩家位之后找 票为0的位置，可能包括在玩的player
                        if(p0 < 0)//全有票
                            melon = 0;
                        else
                            melon = r.game.q_capacity - p0;

                        tickets =  r.game.q_capacity - melon - CONST.concurrent_player_max;

                        const err = utl_who_do_thing_with_what(data.user_id, "try join", data.catcherID, data.ticket?data.ticket:"melon", "but pity");
    //                    const err = `User"${data.user_id}" try join game"${data.catcherID}", but pity.`;
                        log.info("rpc_catcher_participate: " + err);
                        return response.send({ full: `${r.game.q_capacity}|${CONST.concurrent_player_max}|${tickets}|${melon}` });
                    }
                }
            }

        },


        //有票据 但是检查出错
        (msg) => {
            const err = `User"${data.user_id}"'s ticket"${data.ticket}" rejected: ${msg}.`;
            log.error("rpc_catcher_participate: " + err);
            return response.error(err);
        });

}



//处理票据的逻辑，票据也可以是空，若是则立即resolve；
//票据存在，且处理失败的情况下，才会调用reject
function catcher_ticket_handle(client, user_id, ticket) {

    return new Promise(function(resolve, reject) {

        if (undefined === ticket || '' === ticket) {
            return resolve();
        }

        /*	todo  获取用户信息，检查余额，暂扣款
                var record = client.record.getRecord(str_record_catcher);
                record.whenReady(function() {
        	        resolve();
        		    log.info(`${user_id} payed for catcher.`);

        		    reject();
                });
        */
        resolve();
    });


}

function utl_who_do_thing_with_what(who, doo, what, sth, post){
    if(post)
        return `'${who}' ${doo} '${what}' with '${sth}',${post}.`;
    else
        return `'${who}' ${doo} '${what}' with '${sth}'.`;
}

//该函数将返回 字段 在 数组中的所有位置(数组)，类似indexOf
function utl_queue_pos(find, series) {
    var len = series.length
    if (len == 0)
        return [];
    else {
        var out = [],
            idx = 0;
        while ((idx = series.indexOf(find, idx)) > -1) {
            out.push(idx);
            if (idx < len - 1)
                idx += 1;
            else
                break;
        }
        return out;
    }
}
/*
for test
var s = []; //[]
console.log(utl_queue_pos('x', s));
 s = ['x']; //[0]
console.log(utl_queue_pos('x', s));
 s = ['a']; //[]
console.log(utl_queue_pos('x', s));
 s = ['x', 'a']; //[0]
console.log(utl_queue_pos('x', s));
 s = ['a', 'x']; //[1]
console.log(utl_queue_pos('x', s));
 s = ['x', 'x'];	//[0,1]
console.log(utl_queue_pos('x', s));
 s = ['x', 'a', 'a', 'a'];	//[0]
console.log(utl_queue_pos('x', s));
 s = ['a', 'a', 'a', 'x'];	//[3]
console.log(utl_queue_pos('x', s));
 s = ['x', 'a', 'x', 'a'];	//[0,2]
console.log(utl_queue_pos('x', s));
*/







//初始化
function catcher_camera_reset(catcherID) {
    console.log("check device ing...");
    return {status: "online"};
}

function catcher_queue_reset(catcherID) {
    return [];
}

function catcher_qscreen_reset(catcherID) {
    return ["zH32sD", "", "..."];
}

function catcher_stream_reset(catcherID) {
    return `rtmp://live.hkstv.hk.lxdns.com/live/hks`;
}

function catcher_params_info(catcherID) {
    return { queue: {rest:3, driver:1, wait:10, free:2} };    //队列信息
}
