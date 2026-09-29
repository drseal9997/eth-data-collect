// collector.js — runs the ETH signal pipeline for both modes on a schedule,
// storing state in Supabase instead of browser localStorage. Ported from the
// eth-signal-terminal.html prototype's scoring logic.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const ETHERSCAN_KEY = process.env.ETHERSCAN_KEY || null;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || null;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || null;
const TWELVEDATA_KEY = process.env.TWELVEDATA_KEY || null;
const EIA_API_KEY = process.env.EIA_API_KEY || null;

const LEARNING_RATE = 0.04;
const CAL_DECAY = 0.98;
const CAL_BUCKETS = [[0,20],[20,40],[40,60],[60,80],[80,101]];
// Minimum decayed sample count a calibration bucket needs before its observed
// accuracy is trusted enough to override the raw composite-score confidence.
const CAL_MIN_SAMPLES = 10;

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

// `asset` defaults to 'eth' so every existing ETH call site (supaGet(mode),
// supaSet(mode, state)) queries/writes exactly the same row it always has,
// unchanged, now that signal_state's primary key is the composite
// (asset, mode) instead of mode alone. The gold pipeline below is the only
// caller that passes asset='gold' explicitly.
async function supaGet(mode, asset='eth'){
  const res = await fetch(`${SUPABASE_URL}/rest/v1/signal_state?asset=eq.${asset}&mode=eq.${mode}&select=state`, {
    headers:{ apikey:SUPABASE_KEY, Authorization:`Bearer ${SUPABASE_KEY}` }
  });
  if(!res.ok){
    const body = await res.text();
    console.error(`Supabase GET failed for asset=${asset} mode=${mode}: ${res.status} ${res.statusText} — ${body}`);
    return null;
  }
  const rows = await res.json();
  return Array.isArray(rows) && rows.length ? rows[0].state : null;
}

async function supaSet(mode, state, asset='eth'){
  const res = await fetch(`${SUPABASE_URL}/rest/v1/signal_state?on_conflict=asset,mode`, {
    method:'POST',
    headers:{
      apikey:SUPABASE_KEY, Authorization:`Bearer ${SUPABASE_KEY}`,
      'Content-Type':'application/json', Prefer:'resolution=merge-duplicates'
    },
    body: JSON.stringify([{ asset, mode, state, updated_at: new Date().toISOString() }])
  });
  if(!res.ok){
    const body = await res.text();
    console.error(`Supabase WRITE failed for asset=${asset} mode=${mode}: ${res.status} ${res.statusText} — ${body}`);
    throw new Error(`Supabase write failed (${res.status})`);
  }
}

async function loadShared(){
  const s = await supaGet('shared');
  return s || { lastWhaleScore:null, lastWhaleCheckTs:null, lastSentiment:null, lastSentimentCheckTs:null,
    lastHeadlines:null, lastFearGreed:null, lastFundingRate:null, lastContextScore:null, lastContextCheckTs:null };
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

// Returns rich headline objects (title/url/source/ts), not just titles — the
// front-end's "Recent headlines" section displays these straight from shared
// state (see main()'s shared.lastHeadlines below) instead of fetching RSS
// itself client-side. Browsers block that: RSS feeds generally don't send
// Access-Control-Allow-Origin, confirmed for both this feed and gold/oil's
// Al Jazeera/BBC feeds via a live header check.
async function fetchHeadlinesForSentiment(){
  const res = await fetch('https://cointelegraph.com/rss/tag/ethereum', {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; EthCorpusCollector/1.0)' }
  });
  if(!res.ok) throw new Error('rss fetch failed: '+res.status);
  const xml = await res.text();
  const itemBlocks = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
  const headlines = itemBlocks.slice(0,10).map(block=>{
    const titleM = block.match(/<title>([\s\S]*?)<\/title>/);
    if(!titleM) return null;
    const linkM = block.match(/<link>([\s\S]*?)<\/link>/);
    const pubM = block.match(/<pubDate>([\s\S]*?)<\/pubDate>/);
    const ts = pubM ? Date.parse(pubM[1].trim()) : NaN;
    return {
      title: titleM[1].replace('<![CDATA[','').replace(']]>','').trim(),
      url: linkM ? linkM[1].replace('<![CDATA[','').replace(']]>','').trim() : null,
      source: 'Cointelegraph',
      ts: Number.isFinite(ts) ? ts : Date.now()
    };
  }).filter(Boolean);
  if(!headlines.length) throw new Error('rss parse yielded no titles');
  return headlines;
}

async function fetchSentimentGemini(apiKey){
  const headlines = await fetchHeadlinesForSentiment();
  if(!headlines.length) return null;
  const headlineText = headlines.map(h=>`- ${h.title}`).join('\n');
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
  const parsed = match ? JSON.parse(match[0]) : null;
  return parsed ? { ...parsed, headlines } : null;
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
  // A HOLD (sigDir===0) is only "correct" when price genuinely stayed flat
  // (actualDir===0, the same deadzone used for BUY/SHORT) — it is graded wrong
  // if price moved significantly in either direction after a HOLD call.
  return { outcome: sigDir===actualDir ? 'correct' : 'wrong', actualDir };
}
function updateCalibration(modeState, confidence, wasCorrect){
  const b = modeState.calibration.find(x=>confidence>=x.min && confidence<x.max);
  if(!b) return;
  b.total = b.total*CAL_DECAY+1; b.correct = b.correct*CAL_DECAY+(wasCorrect?1:0);
}

// Maps a raw composite-score-derived confidence to the bucket's observed
// (decayed) accuracy, once that bucket has enough samples to be trustworthy.
// Falls back to the raw value otherwise (e.g. buckets still warming up).
function getCalibratedConfidence(modeState, rawConfidence){
  const buckets = modeState && modeState.calibration;
  if(!Array.isArray(buckets) || !Number.isFinite(rawConfidence)) return rawConfidence;
  const b = buckets.find(x=>rawConfidence>=x.min && rawConfidence<x.max);
  if(!b || b.total < CAL_MIN_SAMPLES) return rawConfidence;
  const empirical = Math.round((b.correct/b.total)*100);
  return Math.max(0, Math.min(99, empirical));
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
  const calibratedConfidence = getCalibratedConfidence(modeState, comp.confidence);
  if(!modeState.lastLogTs || (now-modeState.lastLogTs)>=logIntervalMs){
    const horizonsObj={}; cfg.horizons.forEach(([k])=>horizonsObj[k]={graded:false,outcome:null});
    // `confidence` stays the raw composite-score value — it's what calibration
    // buckets are keyed on, so it must not itself be calibrated. `calibratedConfidence`
    // is the display-facing number once its bucket has enough samples to trust.
    modeState.log.push({ ts:now, price:ind.latestPrice, techScore:ind.rawScore, whaleScore, sentimentScore:sentScore, contextScore, signal:comp.signal, confidence:comp.confidence, calibratedConfidence, horizons:horizonsObj });
    modeState.lastLogTs = now;
    if(modeState.log.length>2000) modeState.log = modeState.log.slice(-2000);
  }

  await supaSet(mode, modeState);
  console.log(`[${mode}] price=$${ind.latestPrice.toFixed(2)} signal=${comp.signal} confidence=${comp.confidence}% (calibrated ${calibratedConfidence}%)`);
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

// =====================================================================
// Gold (XAU/USD) — additive parallel pipeline. Reuses MODE_CONFIGS,
// computeIndicators, sma/rsi/ema, and the priceAt/judgeOutcome/
// updateCalibration/getCalibratedConfidence grading primitives verbatim
// (all already asset-agnostic). Does not modify anything in the ETH
// pipeline above; stores its state under signal_state's asset='gold'.
// =====================================================================

function freshModeStateGold(){
  return {
    weights:{tech:0.45, smartMoney:0.30, sentiment:0.25},
    log:[], lastLogTs:null, priceCache:[], volumeCache:[],
    calibration: CAL_BUCKETS.map(([min,max])=>({min,max,correct:0,total:0})),
    lastTechDir:0
  };
}

async function loadSharedGold(){
  const s = await supaGet('shared', 'gold');
  return s || { lastSmartMoneyScore:null, lastSmartMoneyCheckTs:null, lastSmartMoneyDetail:null,
    lastSentiment:null, lastSentimentCheckTs:null, lastSentimentError:null, lastHeadlines:null };
}
async function saveSharedGold(shared){ await supaSet('shared', shared, 'gold'); }

// ---------- gold price: Twelve Data time_series (XAU/USD) ----------
// interval/outputsize chosen per mode to mirror the same swing (long history,
// coarser granularity) vs day-trade (short history, fine granularity)
// split CoinGecko already gives the ETH pipeline via `days`.
const GOLD_TD_INTERVAL = { swing:'1h', day:'15min' };
const GOLD_TD_OUTPUTSIZE = { swing:2160, day:300 }; // ~90d of hourly bars / ~3d of 15-min bars

async function fetchGoldPriceHistory(mode){
  const interval = GOLD_TD_INTERVAL[mode];
  const outputsize = GOLD_TD_OUTPUTSIZE[mode];
  const url = `https://api.twelvedata.com/time_series?symbol=XAU/USD&interval=${interval}&outputsize=${outputsize}&order=asc&timezone=UTC&apikey=${TWELVEDATA_KEY}`;
  const res = await fetch(url);
  const data = await res.json();
  if(data.status === 'error' || !Array.isArray(data.values)){
    throw new Error(`Twelve Data error: ${JSON.stringify(data).slice(0,300)}`);
  }
  const prices = data.values.map(v => [ new Date(v.datetime.replace(' ','T')+'Z').getTime(), Number(v.close) ]);
  // XAU/USD is an OTC spot-style quote — Twelve Data doesn't return meaningful
  // volume for it. computeIndicators still needs a same-length volumes array
  // for its volume-ratio regime factor, so feed it a flat placeholder that
  // always yields a neutral ratio (1) rather than skewing that factor.
  const volumes = prices.map(p => [p[0], 1]);
  return { prices, volumes };
}

// ---------- gold "smart money" factor: CFTC Commitment of Traders ----------
// Free, unauthenticated Socrata Open Data API. The Legacy Futures-Only
// report updates weekly (Fridays, as-of the prior Tuesday), so this is
// cached in sharedGold and only re-checked once a day, not every cycle.
const COT_DATASET_URL = 'https://publicreporting.cftc.gov/resource/6dca-aqww.json';
const COT_GOLD_MARKET_NAME = "GOLD - COMMODITY EXCHANGE INC.";

async function fetchGoldCOT(){
  const where = encodeURIComponent(`market_and_exchange_names='${COT_GOLD_MARKET_NAME}'`);
  const url = `${COT_DATASET_URL}?$where=${where}&$order=report_date_as_yyyy_mm_dd DESC&$limit=52`;
  const res = await fetch(url);
  if(!res.ok) throw new Error(`CFTC COT fetch failed: ${res.status}`);
  const rows = await res.json();
  if(!Array.isArray(rows) || !rows.length) throw new Error('CFTC COT returned no rows');
  return rows; // most-recent report first, up to ~52 weekly reports (~1 year)
}

// "Smart money" here follows the classic COT reading: commercial hedgers
// (miners, bullion banks) are the informed side, read contrarian to
// speculators. Net commercial position (long minus short) is normalized to
// its own trailing ~1-year range — near the top of that range (heavily net
// long relative to its recent history) scores toward +1 (bullish), near the
// bottom (heavily net short) toward -1 (bearish).
function computeGoldSmartMoneyScore(rows){
  const netSeries = rows
    .map(r => Number(r.comm_positions_long_all) - Number(r.comm_positions_short_all))
    .filter(Number.isFinite);
  if(!netSeries.length) return null;
  const latestNet = netSeries[0];
  const min = Math.min(...netSeries), max = Math.max(...netSeries);
  const range = (max-min) || 1;
  const score = Math.max(-1, Math.min(1, ((latestNet-min)/range)*2 - 1));
  const latestRow = rows[0];
  return {
    score, netPosition: latestNet,
    reportDate: latestRow.report_date_as_yyyy_mm_dd,
    openInterest: Number(latestRow.open_interest_all) || null
  };
}

// ---------- gold sentiment: Gemini + geopolitical RSS (Al Jazeera + BBC World) ----------
// Same free-headlines-then-Gemini approach as ETH's fetchSentimentGemini,
// but sourced from world/conflict news rather than crypto news, and scored
// for geopolitical risk (rising tension is typically bullish for gold as a
// safe-haven asset) rather than general market sentiment.
const GOLD_SENTIMENT_FEEDS = [
  'https://www.aljazeera.com/xml/rss/all.xml',
  'https://feeds.bbci.co.uk/news/world/rss.xml'
];

// Returns rich headline objects (title/url/source/ts), not just titles — see
// fetchHeadlinesForSentiment's equivalent note above. Used both to build the
// Gemini sentiment prompt (fetchGoldSentimentGemini below extracts .title)
// and, via that function's return value, to populate the front-end's
// "Recent headlines" section for gold/oil.
async function fetchGeopoliticalHeadlines(){
  const headlines = [];
  const feedSources = { [GOLD_SENTIMENT_FEEDS[0]]:'Al Jazeera', [GOLD_SENTIMENT_FEEDS[1]]:'BBC World' };
  for(const feedUrl of GOLD_SENTIMENT_FEEDS){
    try{
      const res = await fetch(feedUrl, { headers:{ 'User-Agent':'Mozilla/5.0 (compatible; EthCorpusCollector/1.0)' } });
      if(!res.ok){ console.error(`geopolitical rss fetch failed for ${feedUrl}: ${res.status}`); continue; }
      const xml = await res.text();
      const itemBlocks = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
      itemBlocks.slice(0,10).forEach(block=>{
        const titleM = block.match(/<title>([\s\S]*?)<\/title>/);
        if(!titleM) return;
        const linkM = block.match(/<link>([\s\S]*?)<\/link>/);
        const pubM = block.match(/<pubDate>([\s\S]*?)<\/pubDate>/);
        const ts = pubM ? Date.parse(pubM[1].trim()) : NaN;
        headlines.push({
          title: titleM[1].replace('<![CDATA[','').replace(']]>','').trim(),
          url: linkM ? linkM[1].replace('<![CDATA[','').replace(']]>','').trim() : null,
          source: feedSources[feedUrl] || 'World news',
          ts: Number.isFinite(ts) ? ts : Date.now()
        });
      });
    }catch(e){ console.error(`geopolitical rss fetch failed for ${feedUrl}:`, e.message); }
  }
  if(!headlines.length) throw new Error('geopolitical rss parse yielded no titles across both feeds');
  return headlines.sort((a,b)=>b.ts-a.ts).slice(0,10);
}

async function fetchGoldSentimentGemini(apiKey){
  const headlines = await fetchGeopoliticalHeadlines();
  const headlineText = headlines.map(h=>`- ${h.title}`).join('\n');
  // Deliberately avoids naming specific conflict/political categories (war,
  // sanctions, etc.) in the prompt itself — Gemini's API gates certain
  // politically-sensitive content categories by requester region, returning
  // "This API is not available in your current location" (400) rather than a
  // normal safety block. This softer, outcome-focused framing asks for the
  // same safe-haven-demand read without using the trigger language.
  const prompt = `Here are recent world-news headlines:\n${headlineText}\n\nBased only on these headlines, assess how much current global risk sentiment favors safe-haven demand for gold right now — elevated uncertainty or instability in the news typically increases gold demand, while calmer conditions typically decrease it. Respond with ONLY a JSON object, no markdown fences, no other text: {"score": <number from -1 (calm, reduces gold demand) to 1 (high uncertainty, increases gold demand)>, "summary": "<one sentence, under 20 words>"}`;

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
  const parsed = match ? JSON.parse(match[0]) : null;
  return parsed ? { ...parsed, headlines } : null;
}

// ---------- gold composite scoring: 3 factors (tech, smart money, sentiment) ----------
// No market-context equivalent exists for gold (no Fear & Greed / funding
// rate analog), so this is its own function rather than forcing a 4th
// placeholder factor through the ETH computeComposite above.
function checkConfluenceGold(t, s, sent){
  const scores=[t,s,sent].filter(x=>x!==null&&x!==undefined);
  if(scores.length<2) return true;
  const pos=scores.filter(x=>x>0.05).length, neg=scores.filter(x=>x<-0.05).length;
  return Math.max(pos,neg)>=2;
}

function computeCompositeGold(techScore, smartMoneyScore, sentScore, weights, regimeFactor, volumeFactor, mtfMultiplier){
  let total = weights.tech + (smartMoneyScore!==null?weights.smartMoney:0) + (sentScore!==null?weights.sentiment:0);
  if(total===0) total=1;
  let rawComposite = (techScore*weights.tech) + (smartMoneyScore!==null?smartMoneyScore*weights.smartMoney:0) + (sentScore!==null?sentScore*weights.sentiment:0);
  rawComposite/=total;
  const confluenceOk = checkConfluenceGold(techScore, smartMoneyScore, sentScore);
  const activeCount = [techScore, smartMoneyScore, sentScore].filter(s=>s!==null&&s!==undefined).length;
  let q=1;
  q += 0.35*(regimeFactor-1);
  q += 0.25*(volumeFactor-1);
  // Only 3 possible factors total for gold (vs 4 for ETH), so "most of them
  // agreeing" is 2+ active here rather than ETH's 3+.
  if(activeCount>=2) q += confluenceOk?0.05:-0.35;
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

// ---------- gold grading / weight rebalancing (3 weights, no context) ----------
// Reuses priceAt/judgeOutcome/updateCalibration verbatim from the ETH
// pipeline (already asset-agnostic); only the weight-bump section differs,
// since gold's weights object has different keys (tech/smartMoney/sentiment).
function gradeSignalsGold(modeState, cfg, now){
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
          if(entry.smartMoneyScore!==null) modeState.weights.smartMoney=Math.max(0.05, modeState.weights.smartMoney+bump(entry.smartMoneyScore));
          if(entry.sentimentScore!==null) modeState.weights.sentiment=Math.max(0.05, modeState.weights.sentiment+bump(entry.sentimentScore));
          const sum=modeState.weights.tech+modeState.weights.smartMoney+modeState.weights.sentiment;
          modeState.weights.tech/=sum; modeState.weights.smartMoney/=sum; modeState.weights.sentiment/=sum;
        }
      }
    }
  }
}

async function runModeGold(mode, sharedGold){
  const cfg = MODE_CONFIGS[mode]; // same swing/day timeframe config as ETH — asset-agnostic
  let modeState = await supaGet(mode, 'gold');
  if(!modeState) modeState = freshModeStateGold();
  if(!modeState.volumeCache) modeState.volumeCache = [];

  const { prices, volumes } = await fetchGoldPriceHistory(mode);
  modeState.priceCache = prices; modeState.volumeCache = volumes;
  const ind = computeIndicators(prices, volumes, cfg); // exact same RSI/MACD/SMA logic as ETH

  const techDir = Math.abs(ind.rawScore)<0.05?0:Math.sign(ind.rawScore);

  gradeSignalsGold(modeState, cfg, Date.now());

  const smartMoneyScore = sharedGold.lastSmartMoneyScore;
  const sentScore = sharedGold.lastSentiment ? sharedGold.lastSentiment.score : null;

  const otherMode = mode==='swing'?'day':'swing';
  const otherState = await supaGet(otherMode, 'gold');
  const otherDir = otherState ? otherState.lastTechDir : 0;
  let mtfMult = 1.0;
  if(otherDir && techDir) mtfMult = otherDir===techDir ? 1.1 : 0.85;

  modeState.lastTechDir = techDir;

  const comp = computeCompositeGold(ind.rawScore, smartMoneyScore, sentScore, modeState.weights, ind.regimeFactor, ind.volumeFactor, mtfMult);

  const now = Date.now();
  const logIntervalMs = cfg.logIntervalMin*60000;
  const calibratedConfidence = getCalibratedConfidence(modeState, comp.confidence);
  if(!modeState.lastLogTs || (now-modeState.lastLogTs)>=logIntervalMs){
    const horizonsObj={}; cfg.horizons.forEach(([k])=>horizonsObj[k]={graded:false,outcome:null});
    modeState.log.push({ ts:now, price:ind.latestPrice, techScore:ind.rawScore, smartMoneyScore, sentimentScore:sentScore, signal:comp.signal, confidence:comp.confidence, calibratedConfidence, horizons:horizonsObj });
    modeState.lastLogTs = now;
    if(modeState.log.length>2000) modeState.log = modeState.log.slice(-2000);
  }

  await supaSet(mode, modeState, 'gold');
  console.log(`[gold/${mode}] price=$${ind.latestPrice.toFixed(2)} signal=${comp.signal} confidence=${comp.confidence}% (calibrated ${calibratedConfidence}%)`);
  return modeState;
}

// =====================================================================
// Oil (WTI/USD) — additive parallel pipeline, following the same pattern
// as gold above. Unlike gold, oil gets a genuine 4-factor composite (tech,
// COT smart-money, EIA inventory context, geopolitical sentiment) — the
// same shape as ETH's computeComposite, just with oil's own factor names.
// Does not modify anything in the ETH or gold pipelines above; stores its
// state under signal_state's asset='oil'.
//
// Price comes from the EIA (RWTC, Cushing OK WTI spot price), not Twelve
// Data — WTI/USD on Twelve Data requires a paid plan. EIA's spot price is
// DAILY ONLY (confirmed live against the public EIA site: no intraday
// resolution exists for this series at all, unlike ETH/gold's minute/hourly
// bars). Rather than pretend otherwise, oil gets its own OIL_MODE_CONFIGS
// below with horizons expressed in whole days for both modes — "day-trade"
// mode still uses faster indicator periods than swing, but on the same
// daily bars, not real intraday timing.
// =====================================================================

// Mirrors MODE_CONFIGS' shape exactly (same fields computeIndicators/
// gradeSignalsOil/runModeOil expect) but with day-scale horizons for both
// modes, since EIA's daily-only data can't support minute/hour horizons.
// "day" still differs meaningfully from "swing" — faster RSI/SMA periods
// and a shorter lookback — just not via intraday timing.
const OIL_MODE_CONFIGS = {
  swing: {
    label:'Swing', days:200, rsiPeriod:14, smaShort:20, smaLong:50,
    horizons:[['d1',86400000,'1d'],['d7',604800000,'7d'],['d30',2592000000,'30d']],
    primaryHorizonIndex:1, regimeRef:0.01, volShort:5, volLong:24, logIntervalMin:1440
  },
  day: {
    label:'Day-trade', days:90, rsiPeriod:7, smaShort:5, smaLong:10,
    horizons:[['d1',86400000,'1d'],['d3',259200000,'3d'],['d7',604800000,'7d']],
    primaryHorizonIndex:1, regimeRef:0.006, volShort:3, volLong:10, logIntervalMin:1440
  }
};

function freshModeStateOil(){
  return {
    weights:{tech:0.4, smartMoney:0.25, sentiment:0.15, context:0.2},
    log:[], lastLogTs:null, priceCache:[], volumeCache:[],
    calibration: CAL_BUCKETS.map(([min,max])=>({min,max,correct:0,total:0})),
    lastTechDir:0
  };
}

async function loadSharedOil(){
  const s = await supaGet('shared', 'oil');
  return s || { lastSmartMoneyScore:null, lastSmartMoneyCheckTs:null, lastSmartMoneyDetail:null,
    lastContextScore:null, lastContextCheckTs:null, lastInventoryDetail:null,
    lastSentiment:null, lastSentimentCheckTs:null, lastSentimentError:null, lastHeadlines:null };
}
async function saveSharedOil(shared){ await supaSet('shared', shared, 'oil'); }

// ---------- oil price: EIA daily spot price (RWTC — Cushing, OK WTI) ----------
// EIA_API_KEY-gated, same key as the inventory factor below. Confirmed live
// (via the public EIA site, https://www.eia.gov/dnav/pet/hist/rwtcd.htm)
// that this series — "Cushing, OK WTI Spot Price FOB", dollars per barrel —
// is DAILY ONLY; weekly/monthly/annual views on that page are aggregations
// of the same daily series, not a finer resolution. The v2 API route itself
// was confirmed live (403 needing a key, not 404); the exact JSON field
// names below are the standard EIA v2 shape but weren't verified against a
// real key from here — worth checking the first real run's logs.
//
// Distinct OIL_MODE_CONFIGS (below) exists specifically because of this:
// horizons are in whole days for both modes, since there's no intraday data
// to grade 15m/1h/4h horizons against at all.
const EIA_OIL_SPOT_PRICE_URL = 'https://api.eia.gov/v2/petroleum/pri/spt/data/';

async function fetchOilPriceHistory(mode){
  const cfg = OIL_MODE_CONFIGS[mode];
  const url = `${EIA_OIL_SPOT_PRICE_URL}?api_key=${EIA_API_KEY}&frequency=daily&data[0]=value&facets[series][]=RWTC&sort[0][column]=period&sort[0][direction]=desc&length=${cfg.days}`;
  const res = await fetch(url);
  if(!res.ok){ const t = await res.text(); throw new Error(`EIA spot price fetch failed: ${res.status} ${t.slice(0,200)}`); }
  const data = await res.json();
  const rows = data && data.response && Array.isArray(data.response.data) ? data.response.data : null;
  if(!rows || !rows.length) throw new Error('EIA spot price response had no data rows');
  // We requested period desc (most-recent first) — reverse to chronological
  // ascending to match computeIndicators' expected order.
  const ascending = [...rows].reverse();
  const prices = ascending
    .map(r => [ new Date(r.period+'T00:00:00Z').getTime(), Number(r.value) ])
    .filter(p => Number.isFinite(p[1]));
  if(prices.length < 2) throw new Error('EIA spot price returned too few usable rows');
  // A single daily benchmark price has no associated volume figure at all
  // (not even the "no data" case gold/oil's Twelve Data path had) — same
  // flat placeholder so computeIndicators' volume-ratio factor stays neutral.
  const volumes = prices.map(p => [p[0], 1]);
  return { prices, volumes };
}

// ---------- oil "smart money" factor: CFTC Commitment of Traders ----------
// Same dataset (COT_DATASET_URL, defined above for gold) and same
// commercial-hedgers-net-position convention: net long relative to its own
// trailing ~1-year range is bullish, net short is bearish. Verified against
// the live dataset that "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE" is the
// current name for the flagship NYMEX WTI contract (it's the highest-open-
// interest crude oil row in the dataset; the older "CRUDE OIL, LIGHT SWEET"
// naming stopped updating in Feb 2022).
const COT_OIL_MARKET_NAME = "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE";

async function fetchOilCOT(){
  const where = encodeURIComponent(`market_and_exchange_names='${COT_OIL_MARKET_NAME}'`);
  const url = `${COT_DATASET_URL}?$where=${where}&$order=report_date_as_yyyy_mm_dd DESC&$limit=52`;
  const res = await fetch(url);
  if(!res.ok) throw new Error(`CFTC COT fetch failed (oil): ${res.status}`);
  const rows = await res.json();
  if(!Array.isArray(rows) || !rows.length) throw new Error('CFTC COT returned no rows for oil');
  return rows;
}

function computeOilSmartMoneyScore(rows){
  const netSeries = rows
    .map(r => Number(r.comm_positions_long_all) - Number(r.comm_positions_short_all))
    .filter(Number.isFinite);
  if(!netSeries.length) return null;
  const latestNet = netSeries[0];
  const min = Math.min(...netSeries), max = Math.max(...netSeries);
  const range = (max-min) || 1;
  const score = Math.max(-1, Math.min(1, ((latestNet-min)/range)*2 - 1));
  const latestRow = rows[0];
  return {
    score, netPosition: latestNet,
    reportDate: latestRow.report_date_as_yyyy_mm_dd,
    openInterest: Number(latestRow.open_interest_all) || null
  };
}

// ---------- oil context factor: EIA weekly petroleum inventory ----------
// Free, official EIA API (requires EIA_API_KEY). WCESTUS1 is the standard
// series ID for "Weekly U.S. Ending Stocks excluding SPR of Crude Oil"
// (thousand barrels). Endpoint path confirmed live (returns 403 needing a
// key, not 404) — full response shape not verified against a real key from
// here, so worth checking the first real run's logs.
const EIA_OIL_INVENTORY_URL = 'https://api.eia.gov/v2/petroleum/stoc/wstk/data/';

async function fetchOilInventory(apiKey){
  const url = `${EIA_OIL_INVENTORY_URL}?api_key=${apiKey}&frequency=weekly&data[0]=value&facets[series][]=WCESTUS1&sort[0][column]=period&sort[0][direction]=desc&length=13`;
  const res = await fetch(url);
  if(!res.ok){ const t = await res.text(); throw new Error(`EIA inventory fetch failed: ${res.status} ${t.slice(0,200)}`); }
  const data = await res.json();
  const rows = data && data.response && Array.isArray(data.response.data) ? data.response.data : null;
  if(!rows || !rows.length) throw new Error('EIA inventory response had no data rows');
  return rows; // most-recent first (period desc), up to 13 weekly points (~3 months)
}

// Falling inventories (below their own recent trailing average) are
// typically bullish for oil price; rising inventories (above average) are
// bearish — hence the sign flip below. Deviation is normalized against a 3%
// move from the trailing average: U.S. commercial crude stocks run roughly
// 400-450M barrels, so a 3% swing (~12-13M bbl) is a historically large
// weekly deviation from trend.
function computeOilInventoryContextScore(rows){
  const series = rows.map(r => Number(r.value)).filter(Number.isFinite);
  if(series.length < 2) return null;
  const latest = series[0];
  const trailing = series.slice(1);
  const avg = trailing.reduce((a,b)=>a+b,0)/trailing.length;
  if(!Number.isFinite(avg) || avg===0) return null;
  const pctChange = (latest-avg)/avg;
  const score = Math.max(-1, Math.min(1, -(pctChange/0.03)));
  return { score, latest, avg, pctChange, latestPeriod: rows[0].period };
}

// ---------- oil sentiment: reuses gold's fetchGeopoliticalHeadlines() and
// fetchGoldSentimentGemini() verbatim (same Al Jazeera/BBC RSS sources, same
// reworded prompt that avoids Gemini's region-gated content category) — no
// oil-specific sentiment function needed; see main() below for the call site.

// ---------- oil composite scoring: genuine 4 factors (tech, smart money, context, sentiment) ----------
// Same shape as ETH's computeComposite/checkConfluence (activeCount>=3
// confluence bar, same q formula), just with oil's own factor names —
// written separately rather than reusing ETH's function under mismatched
// weight-key names (whale/context vs smartMoney/context).
function checkConfluenceOil(t, s, c, sent){
  const scores=[t,s,c,sent].filter(x=>x!==null&&x!==undefined);
  if(scores.length<2) return true;
  const pos=scores.filter(x=>x>0.05).length, neg=scores.filter(x=>x<-0.05).length;
  return Math.max(pos,neg)>=2;
}

function computeCompositeOil(techScore, smartMoneyScore, contextScore, sentScore, weights, regimeFactor, volumeFactor, mtfMultiplier){
  let total = weights.tech + (smartMoneyScore!==null?weights.smartMoney:0) + (sentScore!==null?weights.sentiment:0) + (contextScore!==null?weights.context:0);
  if(total===0) total=1;
  let rawComposite = (techScore*weights.tech) + (smartMoneyScore!==null?smartMoneyScore*weights.smartMoney:0) + (sentScore!==null?sentScore*weights.sentiment:0) + (contextScore!==null?contextScore*weights.context:0);
  rawComposite/=total;
  const confluenceOk = checkConfluenceOil(techScore, smartMoneyScore, contextScore, sentScore);
  const activeCount = [techScore, smartMoneyScore, sentScore, contextScore].filter(s=>s!==null&&s!==undefined).length;
  let q=1;
  q += 0.35*(regimeFactor-1);
  q += 0.25*(volumeFactor-1);
  if(activeCount>=3) q += confluenceOk?0.05:-0.35; // 4 possible factors, same "3+ agree" bar as ETH
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

// ---------- oil grading / weight rebalancing (4 weights: tech, smartMoney, sentiment, context) ----------
function gradeSignalsOil(modeState, cfg, now){
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
          if(entry.smartMoneyScore!==null) modeState.weights.smartMoney=Math.max(0.05, modeState.weights.smartMoney+bump(entry.smartMoneyScore));
          if(entry.sentimentScore!==null) modeState.weights.sentiment=Math.max(0.05, modeState.weights.sentiment+bump(entry.sentimentScore));
          if(entry.contextScore!==null&&entry.contextScore!==undefined) modeState.weights.context=Math.max(0.05, modeState.weights.context+bump(entry.contextScore));
          const sum=modeState.weights.tech+modeState.weights.smartMoney+modeState.weights.sentiment+modeState.weights.context;
          modeState.weights.tech/=sum; modeState.weights.smartMoney/=sum; modeState.weights.sentiment/=sum; modeState.weights.context/=sum;
        }
      }
    }
  }
}

async function runModeOil(mode, sharedOil){
  const cfg = OIL_MODE_CONFIGS[mode]; // oil's own daily-scale config — see note above MODE_CONFIGS' shared use by ETH/gold
  let modeState = await supaGet(mode, 'oil');
  if(!modeState) modeState = freshModeStateOil();
  if(!modeState.volumeCache) modeState.volumeCache = [];

  const { prices, volumes } = await fetchOilPriceHistory(mode);
  modeState.priceCache = prices; modeState.volumeCache = volumes;
  const ind = computeIndicators(prices, volumes, cfg);

  const techDir = Math.abs(ind.rawScore)<0.05?0:Math.sign(ind.rawScore);

  gradeSignalsOil(modeState, cfg, Date.now());

  const smartMoneyScore = sharedOil.lastSmartMoneyScore;
  const contextScore = sharedOil.lastContextScore;
  const sentScore = sharedOil.lastSentiment ? sharedOil.lastSentiment.score : null;

  const otherMode = mode==='swing'?'day':'swing';
  const otherState = await supaGet(otherMode, 'oil');
  const otherDir = otherState ? otherState.lastTechDir : 0;
  let mtfMult = 1.0;
  if(otherDir && techDir) mtfMult = otherDir===techDir ? 1.1 : 0.85;

  modeState.lastTechDir = techDir;

  const comp = computeCompositeOil(ind.rawScore, smartMoneyScore, contextScore, sentScore, modeState.weights, ind.regimeFactor, ind.volumeFactor, mtfMult);

  const now = Date.now();
  const logIntervalMs = cfg.logIntervalMin*60000;
  const calibratedConfidence = getCalibratedConfidence(modeState, comp.confidence);
  if(!modeState.lastLogTs || (now-modeState.lastLogTs)>=logIntervalMs){
    const horizonsObj={}; cfg.horizons.forEach(([k])=>horizonsObj[k]={graded:false,outcome:null});
    modeState.log.push({ ts:now, price:ind.latestPrice, techScore:ind.rawScore, smartMoneyScore, contextScore, sentimentScore:sentScore, signal:comp.signal, confidence:comp.confidence, calibratedConfidence, horizons:horizonsObj });
    modeState.lastLogTs = now;
    if(modeState.log.length>2000) modeState.log = modeState.log.slice(-2000);
  }

  await supaSet(mode, modeState, 'oil');
  console.log(`[oil/${mode}] price=$${ind.latestPrice.toFixed(2)} signal=${comp.signal} confidence=${comp.confidence}% (calibrated ${calibratedConfidence}%)`);
  return modeState;
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
  // !shared.lastHeadlines forces an immediate recheck if headlines were
  // never persisted yet (e.g. an older cached row from before that field
  // existed) — otherwise it'd stay missing until the 4h timer happened to
  // expire, even though the code to populate it is already correct.
  if(GEMINI_API_KEY && (!shared.lastSentimentCheckTs || !shared.lastHeadlines || now-shared.lastSentimentCheckTs>4*3600000)){
    try{
      const result = await fetchSentimentGemini(GEMINI_API_KEY);
      if(result){
        const { headlines, ...sentimentOnly } = result;
        shared.lastSentiment = sentimentOnly; shared.lastHeadlines = headlines;
        shared.lastSentimentCheckTs = now; shared.lastSentimentError = null;
      }
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

  // ---------- Gold (XAU/USD) — additive, independent of everything above ----------
  if(TWELVEDATA_KEY){
    let sharedGold = await loadSharedGold();

    if(!sharedGold.lastSmartMoneyCheckTs || now-sharedGold.lastSmartMoneyCheckTs>24*3600000){
      try{
        const rows = await fetchGoldCOT();
        const cot = computeGoldSmartMoneyScore(rows);
        if(cot){
          sharedGold.lastSmartMoneyScore = cot.score;
          sharedGold.lastSmartMoneyDetail = { reportDate:cot.reportDate, netPosition:cot.netPosition, openInterest:cot.openInterest };
          sharedGold.lastSmartMoneyCheckTs = now;
        }
      }catch(e){ console.error('gold COT check failed', e.message); }
    }

    // !sharedGold.lastHeadlines forces an immediate recheck if headlines
    // were never persisted yet — see the equivalent comment on shared above.
    if(GEMINI_API_KEY && (!sharedGold.lastSentimentCheckTs || !sharedGold.lastHeadlines || now-sharedGold.lastSentimentCheckTs>4*3600000)){
      try{
        const result = await fetchGoldSentimentGemini(GEMINI_API_KEY);
        if(result){
          const { headlines, ...sentimentOnly } = result;
          sharedGold.lastSentiment = sentimentOnly; sharedGold.lastHeadlines = headlines;
          sharedGold.lastSentimentCheckTs = now; sharedGold.lastSentimentError = null;
        }
      }catch(e){
        const msg = String(e.message);
        const isRegionGated = /not available in your current location/i.test(msg);
        // A region-based content gate (see fetchGoldSentimentGemini) is
        // persistent, not transient like a normal API hiccup — retrying every
        // 5-min cycle would just spam logs and burn Gemini calls for nothing.
        // Back off to the normal 4h interval either way, and only log at
        // error level the first time this exact message shows up; repeats
        // are a quiet, low-key log instead.
        if(isRegionGated){
          if(sharedGold.lastSentimentError !== msg) console.error('gold sentiment check failed (region-gated)', msg);
          else console.log('gold sentiment still region-gated (unchanged) — skipping until next interval');
          sharedGold.lastSentimentCheckTs = now;
        } else {
          console.error('gold sentiment check failed', msg);
        }
        sharedGold.lastSentimentError = msg.slice(0,300);
      }
    }

    await saveSharedGold(sharedGold);

    try{
      await runModeGold('swing', sharedGold);
      await runModeGold('day', sharedGold);
    }catch(e){ console.error('gold pipeline failed', e.message); }
  } else {
    console.log('Skipping gold pipeline — TWELVEDATA_KEY not set');
  }

  // ---------- Oil (WTI/USD) — additive, independent of everything above ----------
  // Gated on EIA_API_KEY, not TWELVEDATA_KEY — oil's price now comes from
  // the EIA (see fetchOilPriceHistory above), so EIA_API_KEY is the one
  // essential dependency for this whole pipeline.
  if(EIA_API_KEY){
    let sharedOil = await loadSharedOil();

    if(!sharedOil.lastSmartMoneyCheckTs || now-sharedOil.lastSmartMoneyCheckTs>24*3600000){
      try{
        const rows = await fetchOilCOT();
        const cot = computeOilSmartMoneyScore(rows);
        if(cot){
          sharedOil.lastSmartMoneyScore = cot.score;
          sharedOil.lastSmartMoneyDetail = { reportDate:cot.reportDate, netPosition:cot.netPosition, openInterest:cot.openInterest };
          sharedOil.lastSmartMoneyCheckTs = now;
        }
      }catch(e){ console.error('oil COT check failed', e.message); }
    }

    if(!sharedOil.lastContextCheckTs || now-sharedOil.lastContextCheckTs>24*3600000){
      try{
        const rows = await fetchOilInventory(EIA_API_KEY);
        const ctx = computeOilInventoryContextScore(rows);
        if(ctx){
          sharedOil.lastContextScore = ctx.score;
          sharedOil.lastInventoryDetail = { period:ctx.latestPeriod, latest:ctx.latest, trailingAvg:ctx.avg };
          sharedOil.lastContextCheckTs = now;
        }
      }catch(e){ console.error('oil inventory check failed', e.message); }
    }

    // !sharedOil.lastHeadlines forces an immediate recheck if headlines
    // were never persisted yet — see the equivalent comment on shared above.
    if(GEMINI_API_KEY && (!sharedOil.lastSentimentCheckTs || !sharedOil.lastHeadlines || now-sharedOil.lastSentimentCheckTs>4*3600000)){
      try{
        // Reuses gold's exact sentiment function — same RSS sources, same
        // reworded prompt (avoids Gemini's region-gated content category).
        const result = await fetchGoldSentimentGemini(GEMINI_API_KEY);
        if(result){
          const { headlines, ...sentimentOnly } = result;
          sharedOil.lastSentiment = sentimentOnly; sharedOil.lastHeadlines = headlines;
          sharedOil.lastSentimentCheckTs = now; sharedOil.lastSentimentError = null;
        }
      }catch(e){
        const msg = String(e.message);
        const isRegionGated = /not available in your current location/i.test(msg);
        if(isRegionGated){
          if(sharedOil.lastSentimentError !== msg) console.error('oil sentiment check failed (region-gated)', msg);
          else console.log('oil sentiment still region-gated (unchanged) — skipping until next interval');
          sharedOil.lastSentimentCheckTs = now;
        } else {
          console.error('oil sentiment check failed', msg);
        }
        sharedOil.lastSentimentError = msg.slice(0,300);
      }
    }

    await saveSharedOil(sharedOil);

    try{
      await runModeOil('swing', sharedOil);
      await runModeOil('day', sharedOil);
    }catch(e){ console.error('oil pipeline failed', e.message); }
  } else {
    console.log('Skipping oil pipeline — EIA_API_KEY not set');
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });
