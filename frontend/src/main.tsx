import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App.tsx'

// Suppress known Univer internal race condition: calculateAutoHeightInRange
// accesses _worksheetData after dispose() has already nulled it.
window.addEventListener('error', (event) => {
  if (
    event.message?.includes('_worksheetData') ||
    event.message?.includes('calculateAutoHeight')
  ) {
    event.preventDefault()
  }
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </StrictMode>,
)
