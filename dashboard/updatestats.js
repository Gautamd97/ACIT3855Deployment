const PROCESSING_STATS_API_URL = "/processing/stats"

const ANALYZER_API_URL = {
    stats: "/analyzer/stats/stats",
    admission: "/analyzer/hospital/admission/history?index=0",
    capacity: "/analyzer/hospital/capacity/history?index=0"
}

const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const updateCodeDiv = (result, elemId) => {
    document.getElementById(elemId).innerText = JSON.stringify(result)
}
const getLocaleDateStr = () => (new Date()).toLocaleString()

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
    
    makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"))
    makeReq(ANALYZER_API_URL.admission, (result) => updateCodeDiv(result, "event-admission"))
    makeReq(ANALYZER_API_URL.capacity, (result) => updateCodeDiv(result, "event-capacity"))
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    getStats()
    setInterval(() => getStats(), 4000)
}

document.addEventListener('DOMContentLoaded', setup)