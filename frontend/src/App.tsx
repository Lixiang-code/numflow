import { Navigate, Route, Routes } from 'react-router-dom'
import AgentTest from './pages/AgentTest'
import DevDiagnostics from './pages/DevDiagnostics'
import Login from './pages/Login'
import NewProject from './pages/NewProject'
import Projects from './pages/Projects'
import Register from './pages/Register'
import Workbench from './pages/Workbench'
import './App.css'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/projects" replace />} />
      <Route path="/login" element={<Login />} />
      <Route path="/register" element={<Register />} />
      <Route path="/projects" element={<Projects />} />
      <Route path="/projects/new" element={<NewProject />} />
      <Route path="/workbench/:projectId" element={<Workbench />} />
      <Route path="/dev" element={<DevDiagnostics />} />
      <Route path="/agent-test" element={<AgentTest />} />
      <Route path="*" element={<Navigate to="/projects" replace />} />
    </Routes>
  )
}
