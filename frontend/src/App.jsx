import { useState, useEffect } from 'react'
import './index.css'

function App() {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [profiles, setProfiles] = useState([])
  const [selectedProfile, setSelectedProfile] = useState('default')
  
  const apiUrl = import.meta.env.VITE_API_URL || 'http://localhost:8000'

  useEffect(() => {
    const fetchProfiles = async () => {
      try {
        const res = await fetch(`${apiUrl}/profiles`)
        if (res.ok) {
          const data = await res.json()
          setProfiles(data)
        }
      } catch (err) {
        console.error("Failed to fetch profiles:", err)
      }
    }
    fetchProfiles()
  }, [apiUrl])

  const sendMessage = async (e) => {
    e.preventDefault()
    if (!input.trim()) return

    const userMessage = { role: 'user', content: input }
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    try {
      // 1. Create task
      const res = await fetch(`${apiUrl}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          prompt: userMessage.content,
          profile_name: selectedProfile
        })
      })
      const { task_id } = await res.json()

      // 2. Poll for result
      const pollInterval = setInterval(async () => {
        try {
          const statusRes = await fetch(`${apiUrl}/tasks/${task_id}`)
          if (statusRes.ok) {
            const data = await statusRes.json()
            if (data.status === 'completed' || data.status === 'failed') {
              clearInterval(pollInterval)
              setIsLoading(false)
              setMessages(prev => [...prev, { 
                role: 'agent', 
                content: data.result || 'An error occurred.',
                isError: data.status === 'failed'
              }])
            }
          }
        } catch (err) {
          console.error("Polling error:", err)
        }
      }, 1000)

    } catch (err) {
      console.error(err)
      setIsLoading(false)
      setMessages(prev => [...prev, { role: 'agent', content: 'Failed to connect to agent.', isError: true }])
    }
  }

  return (
    <div className="app-container">
      <header className="glass-header">
        <h1>Antigravity Agent</h1>
        <div className="header-controls">
          <select 
            className="profile-selector glass-select"
            value={selectedProfile} 
            onChange={(e) => setSelectedProfile(e.target.value)}
            title="Select Agent Profile"
          >
            {profiles.length > 0 ? (
              profiles.map(p => (
                <option key={p.name} value={p.name}>
                  {p.name.charAt(0).toUpperCase() + p.name.slice(1)} Agent
                </option>
              ))
            ) : (
              <option value="default">Default Agent</option>
            )}
          </select>
          <div className="status-badge">
            <span className="pulse-dot"></span> Online
          </div>
        </div>
      </header>
      
      <main className="chat-container">
        <div className="messages-list">
          {messages.length === 0 && (
            <div className="empty-state">
              <div className="bot-icon-large">🤖</div>
              <h2>How can I help you today?</h2>
              <p>I'm powered by LangGraph & Gemini.</p>
            </div>
          )}
          {messages.map((msg, i) => (
            <div key={i} className={`message-wrapper ${msg.role}`}>
              <div className="avatar">{msg.role === 'user' ? 'U' : '🤖'}</div>
              <div className={`message ${msg.isError ? 'error' : ''}`}>
                {msg.content}
              </div>
            </div>
          ))}
          {isLoading && (
            <div className="message-wrapper agent">
              <div className="avatar">🤖</div>
              <div className="message loading-dots">
                <span></span><span></span><span></span>
              </div>
            </div>
          )}
        </div>
      </main>

      <footer className="input-area">
        <form onSubmit={sendMessage} className="input-form glass-form">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Type your task here..."
            disabled={isLoading}
          />
          <button type="submit" disabled={isLoading || !input.trim()}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="22" y1="2" x2="11" y2="13"></line>
              <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
            </svg>
          </button>
        </form>
      </footer>
    </div>
  )
}

export default App
