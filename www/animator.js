var Animator=new (function(){
	var jobs={
		timeout:{},
		interval:{},
		animation:{}
	}
	var mili=1;
	
	var self=this;
	var assignedTimeoutNum=0;
	var assignedIntervalNum=0;
	var assignedAnimationNum=0;
	
	this.setSpeed=function(m){
		mili=m;
	}
	this.setTimeout=function(func,timeout){
		jobs.timeout[assignedTimeoutNum]={
			func:func,
			time:Date.now()+timeout
		};
		assignedTimeoutNum++;
		return assignedTimeoutNum-1;
	}
	this.setInterval=function(func,interval){

		jobs.interval[assignedIntervalNum]={
			func:func,
			latest:Date.now()-interval,
			interval:interval
		};
		assignedIntervalNum++;
		return assignedIntervalNum-1;
	}
	this.setAnimation=function(func,duration,callback){
		jobs.animation[assignedAnimationNum]={
			func:func,
			time:Date.now(),
			duration:duration,
			callback:callback?callback:function(){}
		};
		assignedAnimationNum++;
		return assignedAnimationNum-1;
	}
	this.clearTimeout=function(key){
		//jobsToClear.timeout.push(key)
		delete jobs.timeout[key];
	}
	this.clearInterval=function(key){
		//jobsToClear.interval.push(key)
		delete jobs.interval[key];
	}
	this.clearAnimation=function(key){
		//jobsToClear.animation.push(key)
		delete jobs.animation[key];
	}
	window.requestAnimationFrame=window.requestAnimationFrame || window.mozRequestAnimationFrame || window.webkitRequestAnimationFrame || window.msRequestAnimationFrame || function(f){setTimeout(f,1);};
	var requestNextFrame=function(){
		var now=Date.now();
		for (var key in jobs.timeout){
			if (jobs.timeout[key].time<now){
				jobs.timeout[key].func();
				self.clearTimeout(key);
			}
		}
		for (var key in jobs.interval){
			if (jobs.interval[key] && jobs.interval[key].latest + jobs.interval[key].interval < now){
				jobs.interval[key].latest=now;
				jobs.interval[key].func();
			}
		}
		for (var key in jobs.animation){
			var pc=Math.min(1, (now - jobs.animation[key].time) / jobs.animation[key].duration);
			jobs.animation[key].func(pc);
			if (pc==1){
				jobs.animation[key].callback ? jobs.animation[key].callback() : null;
				self.clearAnimation(key);
			}
		}
		requestAnimationFrame(requestNextFrame);
	}
	requestNextFrame();
})();