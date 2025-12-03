const PROCESSING_STATS_API_URL = "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8100/stats"

const ANALYZER_API_URL = {
    stats: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/stats",
    admission: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/hospital/admission/history?index=0",
    capacity: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/hospital/capacity/history?index=0"
}

const makeReq = (url, cb) => {
    console.log(`Fetching from: ${url}`)
    fetch(url)
        .then(res => {
            console.log(`Response status for ${url}: ${res.status}`)
            if (!res.ok) {
                throw new Error(`HTTP error! status: ${res.status}`)
            }
            return res.json()
        })
        .then((result) => {
            console.log("Received data from", url, ":", result)
            cb(result);
        })
        .catch((error) => {
            console.error(`Error fetching ${url}:`, error)
            updateErrorMessages(`Failed to fetch from ${url}: ${error.message}`)
        })
}

const updateCodeDiv = (result, elemId) => {
    const elem = document.getElementById(elemId)
    if (elem) {
        elem.innerText = JSON.stringify(result, null, 2)
    } else {
        console.error(`Element ${elemId} not found`)
    }
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
    console.log("Error message creation", id, message)
    const msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.className = "error-message"
    msg.innerHTML = `<p><strong>Error at ${getLocaleDateStr()}</strong></p><code>${message}</code>`
    
    const messagesDiv = document.getElementById("messages")
    messagesDiv.style.display = "block"
    messagesDiv.prepend(msg)
    
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { 
            elem.remove() 
            console.log("Removed error message", id)
        }
    }, 7000)
}

const setup = () => {
    console.log("Dashboard initializing...")
    getStats()
    setInterval(() => getStats(), 4000)
}

document.addEventListener('DOMContentLoaded', setup)







// const PROCESSING_STATS_API_URL = "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8100/stats"

// const ANALYZER_API_URL = {
//     stats: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/stats",
//     admission: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/hospital/admission/history?index=0",
//     capacity: "http://gautamdhoopar3855.westus3.cloudapp.azure.com:8110/hospital/capacity/history?index=0"
// }

// const makeReq = (url, cb) => {
//     fetch(url)
//         .then(res => res.json())
//         .then((result) => {
//             console.log("Received data: ", result)
//             cb(result);
//         }).catch((error) => {
//             updateErrorMessages(error.message)
//         })
// }

// const updateCodeDiv = (result, elemId) => {
//     document.getElementById(elemId).innerText = JSON.stringify(result, null, 2)
// }
// const getLocaleDateStr = () => (new Date()).toLocaleString()

// const getStats = () => {
//     document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
//     makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
//     makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"))
//     makeReq(ANALYZER_API_URL.admission, (result) => updateCodeDiv(result, "event-admission"))
//     makeReq(ANALYZER_API_URL.capacity, (result) => updateCodeDiv(result, "event-capacity"))
// }

// const updateErrorMessages = (message) => {
//     const id = Date.now()
//     console.log("Creation", id)
//     msg = document.createElement("div")
//     msg.id = `error-${id}`
//     msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
//     document.getElementById("messages").style.display = "block"
//     document.getElementById("messages").prepend(msg)
//     setTimeout(() => {
//         const elem = document.getElementById(`error-${id}`)
//         if (elem) { elem.remove() }
//     }, 7000)
// }

// const setup = () => {
//     getStats()
//     setInterval(() => getStats(), 4000)
// }

// document.addEventListener('DOMContentLoaded', setup)