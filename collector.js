// collector.js — runs the ETH signal pipeline for both modes on a schedule,
// storing state in Supabase instead of browser localStorage. Ported from the
// eth-signal-terminal.html prototype's scoring logic.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const ETHERSCAN_KEY = process.env.ETHERSCAN_KEY || null;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || null;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || null;

const LEARNING_RATE = 0.04;
const CAL_DECAY = 0.98;
const CAL_BUCKETS = [[0,20],[20,40],[40,60],[60,80],[80,101]];

const MODE_CONFIGS = {
  swing: {
    label:'Swing', days:90, rsiPeriod:14, smaShort:20, smaLong:50,
    horizons:[['h1',3600000,'1h'],['h24',86400000,'24h'],['d7',604800000,'7d']],
    primaryHorizonIndex:1, regimeRef:0.01, volShort:5, volLong:24, logIntervalMin:60
  },
  day: {
    label:'Day-trade', days:1, rsiPeriod:7, smaShort:9, smaLong:21,
    horizons:[['m15',900000,'15m'],['h1',3600000,'1h'],['h4',14400000,'4h']],
    primaryHorizonIndex:1, regimeRef:0.004, volShort:6, volLong:24, logIntervalMin:15
  }
};

function freshModeState(){
  return {
    weights:{tech:0.4, whale:0.25, sentiment:0.15, context:0.2},
    log:[], lastLogTs:null, priceCache:[], volumeCache:[],
    calibration: CAL_BUCKETS.map(([min,max])=>({min,max,correct:0,total:0})),
    lastTechDir:0
  };
}

async function supaGet(mode){
  const res = await fetch(`${SUPABASE_URL}/rest/v1/signal_state?mode=eq.${mode}&select=state`, {
    headers:{ apikey:SUPABASE_KEY, Authorization:`Bearer ${SUPABASE_KEY}` }
  });
  if(!res.ok){
    const body = await res.text();
    console.error(`Supabase GET failed for mode=${mode}: ${res.status} ${res.statusText} — ${body}`);
    return null;
  }
  const rows = await res.json();
  return Array.isArray(rows) && rows.length ? rows[0].state : null;
}

async function supaSet(mode, state){
  const res = await fetch(`${SUPABASE_URL}/rest/v1/signal_state?on_conflict=mode`, {
    method:'POST',
    headers:{
      apikey:SUPABASE_KEY, Authorization:`Bearer ${SUPABASE_KEY}`,
      'Content-Type':'application/json', Prefer:'resolution=merge-duplicates'
    },
    body: JSON.stringify([{ mode, state, updated_at: new Date().toISOString() }])
  });
  if(!res.ok){
    const body = await res.text();
    console.error(`Supabase WRITE failed for mode=${mode}: ${res.status} ${res.statusText} — ${body}`);
    throw new Error(`Supabase write failed (${res.status})`);
  }
}

async function loadShared(){
  const s = await supaGet('shared');
  return s || { lastWhaleScore:null, lastWhaleCheckTs:null, lastSentiment:null, lastSentimentCheckTs:null,
    lastFearGreed:null, lastFundingRate:null, lastContextScore:null, lastContextCheckTs:null };
}
async function saveShared(shared){ await supaSet('shared', shared); }

function sma(vals, period){
  const out=[];
  for(let i=0;i<vals.length;i++){
    if(i<period-1){ out.push(null); continue; }
    let sum=0; for(let j=i-period+1;j<=i;j++) sum+=vals[j];
    out.push(sum/period);
  }
  return out;
}
function rsi(vals, period){
  let gains=0, losses=0;
  for(let i=1;i<=period;i++){ const d=vals[i]-vals[i-1]; if(d>=0) gains+=d; else losses-=d; }
  let avgGain=gains/period, avgLoss=losses/period;
  const out=new Array(period).fill(null);
  out.push(avgLoss===0?100:100-(100/(1+avgGain/avgLoss)));
  for(let i=period+1;i<vals.length;i++){
    const d=vals[i]-vals[i-1]; const gain=d>0?d:0, loss=d<0?-d:0;
    avgGain=(avgGain*(period-1)+gain)/period; avgLoss=(avgLoss*(period-1)+loss)/period;
    out.push(avgLoss===0?100:100-(100/(1+avgGain/avgLoss)));
  }
  return out;
}
function ema(vals, period){
  const k=2/(period+1); const out=[vals[0]];
  for(let i=1;i<vals.length;i++) out.push(vals[i]*k+out[i-1]*(1-k));
  return out;
}

async function fetchPriceHistory(days){
  const res = await fetch(`https://api.coingecko.com/api/v3/coins/ethereum/market_chart?vs_currency=usd&days=${days}`);
  const data = await res.json();
  return { prices:data.prices, volumes:data.total_volumes };
}

function computeIndicators(prices, volumes, cfg){
  const closes = prices.map(p=>p[1]);
  const vols = volumes.map(v=>v[1]);
  const smaShortArr = sma(closes, cfg.smaShort), smaLongArr = sma(closes, cfg.smaLong);
  const rsiArr = rsi(closes, cfg.rsiPeriod);
  const ema12=ema(closes,12), ema26=ema(closes,26);
  const macdLine = ema12.map((v,i)=>v-ema26[i]);
  const signalLine = ema(macdLine,9);
  const histArr = macdLine.map((v,i)=>v-signalLine[i]);
  const lastRsi=rsiArr[rsiArr.length-1], lastHist=histArr[histArr.length-1];
  const lastShort=smaShortArr[smaShortArr.length-1], lastLong=smaLongArr[smaLongArr.length-1];
  let rawScore=0;
  if(lastRsi<30) rawScore+=0.4; else if(lastRsi>70) rawScore-=0.4; else rawScore+=(50-lastRsi)/50*0.15;
  const normHist=lastHist/(closes[closes.length-1]*0.01);
  rawScore += Math.max(-0.4, Math.min(0.4, normHist*0.4));
  if(lastShort && lastLong) rawScore += lastShort>lastLong?0.2:-0.2;
  rawScore = Math.max(-1, Math.min(1, rawScore));
  const trendStrength = (lastShort&&lastLong)?Math.abs(lastShort-lastLong)/lastLong:0;
  const regimeFactor = Math.max(0.3, Math.min(1.3, trendStrength/cfg.regimeRef));
  const volShortAvg = vols.slice(-cfg.volShort).reduce((a,b)=>a+b,0)/Math.max(1,Math.min(cfg.volShort,vols.length));
  const volLongAvg = vols.slice(-cfg.volLong).reduce((a,b)=>a+b,0)/Math.max(1,Math.min(cfg.volLong,vols.length));
  const volumeRatio = volLongAvg>0?volShortAvg/volLongAvg:1;
  const volumeFactor = Math.max(0.6, Math.min(1.35, 0.75+0.35*(volumeRatio-1)));
  return { rawScore, rsi:lastRsi, hist:lastHist, trendUp:lastShort>lastLong, trendStrength, regimeFactor, volumeRatio, volumeFactor, closes, latestPrice:closes[closes.length-1] };
}

const EXCHANGE_WALLETS = ['0x28C6c06298d514Db089934071355E5743bf21d60','0x71660c4005BA85c37ccec55d0C4493E66Fe775d3'];
async function fetchWhaleFlow(apiKey){
  const cutoff = Date.now()/1000 - 24*3600;
  let inflow=0, outflow=0;
  for(const addr of EXCHANGE_WALLETS){
    const url=`https://api.etherscan.io/api?module=account&action=txlist&address=${addr}&sort=desc&offset=300&page=1&apikey=${apiKey}`;
    const res=await fetch(url); const data=await res.json();
    if(!data.result || !Array.isArray(data.result)) continue;
    for(const tx of data.result){
      if(Number(tx.timeStamp)<cutoff) continue;
      const eth=Number(tx.value)/1e18;
      if(tx.to && tx.to.toLowerCase()===addr.toLowerCase()) inflow+=eth;
      if(tx.from && tx.from.toLowerCase()===addr.toLowerCase()) outflow+=eth;
    }
  }
  const net=inflow-outflow; const denom=Math.max(inflow+outflow,1);
  return { score: Math.max(-1, Math.min(1, -(net/denom))), inflow, outflow };
}

async function fetchMarketContext(){
  const [fgRes, fundingRes] = await Promise.all([
    fetch('https://api.alternative.me/fng/?limit=1'),
    fetch('https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT')
  ]);
  if(!fgRes.ok) throw new Error('fear/greed fetch failed: '+fgRes.status);
  const fgData = await fgRes.json();
  const fgValue = Number(fgData.data[0].value);
  const fgClass = fgData.data[0].value_classification;
  if(!Number.isFinite(fgValue)) throw new Error('fear/greed value invalid');
  const fgScore = Math.max(-1, Math.min(1, (50-fgValue)/50));

  let fundingRate = null, fundingScore = 0;
  if(fundingRes.ok){
    const fundingData = await fundingRes.json();
    const parsedRate = Number(fundingData.lastFundingRate);
    if(Number.isFinite(parsedRate)){
      fundingRate = parsedRate;
      fundingScore = Math.max(-1, Math.min(1, -(fundingRate/0.001)));
    } else {
      console.error('funding rate response invalid:', JSON.stringify(fundingData).slice(0,200));
    }
  } else {
    console.error('funding rate fetch failed:', fundingRes.status);
  }

  const contextScore = fundingRate!==null ? (fgScore+fundingScore)/2 : fgScore;
  return { contextScore, fgValue, fgClass, fundingRate };
}

async function fetchSentiment(){
  if(!ANTHROPIC_API_KEY) return null;
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method:"POST",
    headers:{"Content-Type":"application/json", "x-api-key":ANTHROPIC_API_KEY, "anthropic-version":"2023-06-01"},
    body: JSON.stringify({
      model:"claude-sonnet-4-6", max_tokens:1000,
      messages:[{role:"user", content:"Search for recent news and social media sentiment about Ethereum (ETH) from the last 24-48 hours. Then respond with ONLY a JSON object, no other text, no markdown fences: {\"score\": <number from -1 to 1>, \"summary\": \"<one sentence, under 20 words>\"}"}],
      tools:[{type:"web_search_20250305", name:"web_search"}]
    })
  });
  const data = await response.json();
  if(!data.content) return null;
  const textBlocks = data.content.filter(b=>b.type==="text").map(b=>b.text).join("\n");
  const clean = textBlocks.replace(/```json|```/g,"").trim();
  const jsonMatch = clean.match(/\{[\s\S]*\}/);
  return jsonMatch ? JSON.parse(jsonMatch[0]) : null;
}

// ---------- free sentiment path: Gemini + free CryptoCompare headlines ----------
// Deliberately avoids Gemini's google_search tool, which is billed per query even
// on free-tier accounts. Instead we score sentiment from headlines we already fetch
// for free from a public RSS feed — no API key, no signup, no auth needed at all.

async function fetchHeadlinesForSentiment(){
  const res = await fetch('https://cointelegraph.com/rss/tag/ethereum', {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; EthCorpusCollector/1.0)' }
  });
  if(!res.ok) throw new Error('rss fetch failed: '+res.status);
  const xml = await res.text();
  const itemBlocks = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
  const titles = itemBlocks.slice(0,10).map(block=>{
    const m = block.match(/<title>([\s\S]*?)<\/title>/);
    if(!m) return null;
    return m[1].replace('<![CDATA[','').replace(']]>','').trim();
  }).filter(Boolean);
  if(!titles.length) throw new Error('rss parse yielded no titles');
  return titles;
}

async function fetchSentimentGemini(apiKey){
  const headlines = await fetchHeadlinesForSentiment();
  if(!headlines.length) return null;
  const headlineText = headlines.map(t=>`- ${t}`).join('\n');
  const prompt = `Here are recent Ethereum (ETH) news headlines:\n${headlineText}\n\nBased only on these headlines, respond with ONLY a JSON object, no markdown fences, no other text: {"score": <number from -1 (very bearish) to 1 (very bullish)>, "summary": "<one sentence, under 20 words>"}`;

  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/interactions?key=${apiKey}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ model:'gemini-3.1-flash-lite', input: prompt })
  });
  if(!res.ok){ const t = await res.text(); throw new Error(`Gemini error ${res.status}: ${t}`); }
  const data = await res.json();
  const lastStep = data.steps[data.steps.length-1];
  const textPart = (lastStep.content || []).find(c=>c.type==='text');
  if(!textPart) return null;
  const clean = textPart.text.replace(/```json|```/g,'').trim();
  const match = clean.match(/\{[\s\S]*\}/);
  return match ? JSON.parse(match[0]) : null;
}

function checkConfluence(t,w,s,c){
  const scores=[t,w,s,c].filter(x=>x!==null&&x!==undefined);
  if(scores.length<2) return true;
  const pos=scores.filter(x=>x>0.05).length, neg=scores.filter(x=>x<-0.05).length;
  return Math.max(pos,neg)>=2;
}

function computeComposite(techScore, whaleScore, sentScore, contextScore, weights, regimeFactor, volumeFactor, mtfMultiplier){
  let total = weights.tech + (whaleScore!==null?weights.whale:0) + (sentScore!==null?weights.sentiment:0) + (contextScore!==null?weights.context:0);
  if(total===0) total=1;
  let rawComposite = (techScore*weights.tech) + (whaleScore!==null?whaleScore*weights.whale:0) + (sentScore!==null?sentScore*weights.sentiment:0) + (contextScore!==null?contextScore*weights.context:0);
  rawComposite/=total;
  const confluenceOk = checkConfluence(techScore, whaleScore, sentScore, contextScore);
  const activeCount = [techScore, whaleScore, sentScore, contextScore].filter(s=>s!==null&&s!==undefined).length;
  let q=1;
  q += 0.35*(regimeFactor-1);
  q += 0.25*(volumeFactor-1);
  if(activeCount>=3) q += confluenceOk?0.05:-0.35;
  q += 0.15*(mtfMultiplier-1);
  q = Math.max(0.3, Math.min(1.4, q));
  let adjusted = rawComposite*q;
  adjusted = Number.isFinite(adjusted) ? Math.max(-1, Math.min(1, adjusted)) : 0;
  let signal='HOLD';
  if(adjusted>0.15) signal='BUY'; else if(adjusted<-0.15) signal='SHORT';
  let confidence = Math.min(99, Math.round(Math.abs(adjusted)*100));
  if(!Number.isFinite(confidence)) confidence = 0;
  return { composite:adjusted, signal, confidence, confluenceOk };
}

function priceAt(modeState, ts){
  if(!modeState.priceCache.length) return null;
  let closest=modeState.priceCache[0];
  for(const p of modeState.priceCache) if(Math.abs(p[0]-ts)<Math.abs(closest[0]-ts)) closest=p;
  return closest[1];
}
function judgeOutcome(entry, priceThen){
  const ret=(priceThen-entry.price)/entry.price;
  const actualDir = Math.abs(ret)<0.005?0:Math.sign(ret);
  const sigDir = entry.signal==='BUY'?1:entry.signal==='SHORT'?-1:0;
  return { outcome:(sigDir===actualDir||sigDir===0)?'correct':'wrong', actualDir };
}
function updateCalibration(modeState, confidence, wasCorrect){
  const b = modeState.calibration.find(x=>confidence>=x.min && confidence<x.max);
  if(!b) return;
  b.total = b.total*CAL_DECAY+1; b.correct = b.correct*CAL_DECAY+(wasCorrect?1:0);
}
function gradeSignals(modeState, cfg, now){
  const primaryKey = cfg.horizons[cfg.primaryHorizonIndex][0];
  for(const entry of modeState.log){
    if(!entry.horizons){ entry.horizons={}; cfg.horizons.forEach(([k])=>entry.horizons[k]={graded:false,outcome:null}); }
    for(const [key,ms] of cfg.horizons){
      const h=entry.horizons[key];
      if(!h||h.graded) continue;
      const due=entry.ts+ms;
      if(now<due) continue;
      const priceThen=priceAt(modeState, due);
      if(priceThen===null) continue;
      const {outcome, actualDir} = judgeOutcome(entry, priceThen);
      h.graded=true; h.outcome=outcome;
      if(key===primaryKey){
        updateCalibration(modeState, entry.confidence, outcome==='correct');
        if(actualDir!==0){
          const bump = s=>(s===null||s===undefined)?0:(Math.sign(s)===actualDir?LEARNING_RATE:(Math.sign(s)===-actualDir?-LEARNING_RATE:0));
          modeState.weights.tech=Math.max(0.05, modeState.weights.tech+bump(entry.techScore));
          if(entry.whaleScore!==null) modeState.weights.whale=Math.max(0.05, modeState.weights.whale+bump(entry.whaleScore));
          if(entry.sentimentScore!==null) modeState.weights.sentiment=Math.max(0.05, modeState.weights.sentiment+bump(entry.sentimentScore));
          if(entry.contextScore!==null&&entry.contextScore!==undefined) modeState.weights.context=Math.max(0.05, modeState.weights.context+bump(entry.contextScore));
          const sum=modeState.weights.tech+modeState.weights.whale+modeState.weights.sentiment+modeState.weights.context;
          modeState.weights.tech/=sum; modeState.weights.whale/=sum; modeState.weights.sentiment/=sum; modeState.weights.context/=sum;
        }
      }
    }
  }
}

async function runMode(mode, shared){
  const cfg = MODE_CONFIGS[mode];
  let modeState = await supaGet(mode);
  if(!modeState) modeState = freshModeState();
  if(!modeState.volumeCache) modeState.volumeCache = [];

  const { prices, volumes } = await fetchPriceHistory(cfg.days);
  modeState.priceCache = prices; modeState.volumeCache = volumes;
  const ind = computeIndicators(prices, volumes, cfg);

  const techDir = Math.abs(ind.rawScore)<0.05?0:Math.sign(ind.rawScore);

  gradeSignals(modeState, cfg, Date.now());

  const whaleScore = shared.lastWhaleScore;
  const sentScore = shared.lastSentiment ? shared.lastSentiment.score : null;
  const contextScore = shared.lastContextScore;

  const otherMode = mode==='swing'?'day':'swing';
  const otherState = await supaGet(otherMode);
  const otherDir = otherState ? otherState.lastTechDir : 0;
  let mtfMult = 1.0;
  if(otherDir && techDir) mtfMult = otherDir===techDir ? 1.1 : 0.85;

  modeState.lastTechDir = techDir;

  const comp = computeComposite(ind.rawScore, whaleScore, sentScore, contextScore, modeState.weights, ind.regimeFactor, ind.volumeFactor, mtfMult);

  const now = Date.now();
  const logIntervalMs = cfg.logIntervalMin*60000;
  if(!modeState.lastLogTs || (now-modeState.lastLogTs)>=logIntervalMs){
    const horizonsObj={}; cfg.horizons.forEach(([k])=>horizonsObj[k]={graded:false,outcome:null});
    modeState.log.push({ ts:now, price:ind.latestPrice, techScore:ind.rawScore, whaleScore, sentimentScore:sentScore, contextScore, signal:comp.signal, confidence:comp.confidence, horizons:horizonsObj });
    modeState.lastLogTs = now;
    if(modeState.log.length>2000) modeState.log = modeState.log.slice(-2000);
  }

  await supaSet(mode, modeState);
  console.log(`[${mode}] price=$${ind.latestPrice.toFixed(2)} signal=${comp.signal} confidence=${comp.confidence}%`);
  return modeState;
}

async function fetchAICommentary(apiKey, swingState, dayState, shared){
  const swingLatest = [...(swingState.log||[])].sort((a,b)=>b.ts-a.ts)[0];
  const dayLatest = [...(dayState.log||[])].sort((a,b)=>b.ts-a.ts)[0];
  const prompt = `You are an independent market commentator reviewing a live ETH trading-signal system. Current state:

Swing signal (hours-to-days horizon): ${swingLatest ? `${swingLatest.signal} at ${swingLatest.confidence}% confidence, price $${swingLatest.price.toFixed(2)}` : 'no data yet'}
Day-trade signal (minutes-to-hours horizon): ${dayLatest ? `${dayLatest.signal} at ${dayLatest.confidence}% confidence, price $${dayLatest.price.toFixed(2)}` : 'no data yet'}
Swing factor weights: technical ${Math.round(swingState.weights.tech*100)}%, whale ${Math.round(swingState.weights.whale*100)}%, sentiment ${Math.round(swingState.weights.sentiment*100)}%, context ${Math.round(swingState.weights.context*100)}%
Sentiment: ${shared.lastSentiment ? `score ${shared.lastSentiment.score} — "${shared.lastSentiment.summary}"` : 'not available'}
Market context: Fear & Greed ${shared.lastFearGreed ? `${shared.lastFearGreed.value} (${shared.lastFearGreed.classification})` : 'n/a'}${shared.lastFundingRate!=null ? `, funding rate ${shared.lastFundingRate}` : ''}

Write a short, independent, plain-English take (2-4 sentences) on what's going on right now. Tone: measured and balanced — like an experienced analyst who isn't trying to sell excitement or alarm either way. Don't lead with caution or hedge everything; state what the data actually shows plainly and let it speak for itself. If the swing and day-trade signals disagree, mention it as a neutral, useful fact (different timeframes naturally diverge sometimes) — not as a red flag or reason for concern. Avoid hype language, but also avoid sounding pessimistic or overly cautious by default. Respond with ONLY a JSON object, no markdown fences, no other text: {"text": "<your 2-4 sentence commentary>"}`;

  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/interactions?key=${apiKey}`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ model:'gemini-3.1-flash-lite', input: prompt })
  });
  if(!res.ok){ const t = await res.text(); throw new Error(`Gemini error ${res.status}: ${t}`); }
  const data = await res.json();
  const lastStep = data.steps[data.steps.length-1];
  const textPart = (lastStep.content || []).find(c=>c.type==='text');
  if(!textPart) return null;
  const clean = textPart.text.replace(/```json|```/g,'').trim();
  const match = clean.match(/\{[\s\S]*\}/);
  return match ? JSON.parse(match[0]) : null;
}

async function main(){
  if(!SUPABASE_URL || !SUPABASE_KEY){ console.error('Missing SUPABASE_URL / SUPABASE_KEY'); process.exit(1); }

  let shared = await loadShared();
  const now = Date.now();

  if(ETHERSCAN_KEY && (!shared.lastWhaleCheckTs || now-shared.lastWhaleCheckTs>15*60000)){
    try{
      const flow = await fetchWhaleFlow(ETHERSCAN_KEY);
      shared.lastWhaleScore = flow.score; shared.lastWhaleCheckTs = now;
    }catch(e){ console.error('whale check failed', e.message); }
  }
  if(GEMINI_API_KEY && (!shared.lastSentimentCheckTs || now-shared.lastSentimentCheckTs>4*3600000)){
    try{
      const result = await fetchSentimentGemini(GEMINI_API_KEY);
      if(result){ shared.lastSentiment = result; shared.lastSentimentCheckTs = now; shared.lastSentimentError = null; }
    }catch(e){ console.error('sentiment check failed (gemini)', e.message); shared.lastSentimentError = String(e.message).slice(0,300); }
  } else if(ANTHROPIC_API_KEY && (!shared.lastSentimentCheckTs || now-shared.lastSentimentCheckTs>4*3600000)){
    try{
      const result = await fetchSentiment();
      if(result){ shared.lastSentiment = result; shared.lastSentimentCheckTs = now; }
    }catch(e){ console.error('sentiment check failed (anthropic)', e.message); }
  }
  if(!shared.lastContextCheckTs || now-shared.lastContextCheckTs>10*60000){
    try{
      const ctx = await fetchMarketContext();
      shared.lastFearGreed = { value:ctx.fgValue, classification:ctx.fgClass };
      shared.lastFundingRate = ctx.fundingRate; shared.lastContextScore = ctx.contextScore; shared.lastContextCheckTs = now;
    }catch(e){ console.error('context check failed', e.message); }
  }
  await saveShared(shared);

  const swingState = await runMode('swing', shared);
  const dayState = await runMode('day', shared);

  if(GEMINI_API_KEY && (!shared.lastAICommentaryTs || now-shared.lastAICommentaryTs>3600000)){
    try{
      const commentary = await fetchAICommentary(GEMINI_API_KEY, swingState, dayState, shared);
      if(commentary && commentary.text){
        shared.aiCommentary = commentary.text;
        shared.lastAICommentaryTs = now;
        shared.aiCommentaryError = null;
      }
    }catch(e){
      console.error('ai commentary failed', e.message);
      shared.aiCommentaryError = String(e.message).slice(0,300);
    }
    await saveShared(shared);
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });
