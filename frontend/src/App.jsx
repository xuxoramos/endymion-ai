import React from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import CowList from './components/CowList';
import CowCard from './components/CowCard';
import CreateCowForm from './components/CreateCowForm';
import Dashboard from './components/Dashboard';
import ArchitectureView from './components/ArchitectureView';

function App() {
  const location = useLocation();

  return (
    <div>
      <header className="header">
        <div className="header-content">
          <h1>🐄 Endymion-AI</h1>
          <nav className="nav">
            <Link 
              to="/" 
              className={location.pathname === '/' ? 'active' : ''}
            >
              Cattle List
            </Link>
            <Link 
              to="/dashboard" 
              className={location.pathname === '/dashboard' ? 'active' : ''}
            >
              Dashboard
            </Link>
            <Link 
              to="/create" 
              className={location.pathname === '/create' ? 'active' : ''}
            >
              Add Cow
            </Link>
            <Link 
              to="/architecture" 
              className={location.pathname === '/architecture' ? 'active' : ''}
            >
              Architecture
            </Link>
          </nav>
        </div>
      </header>

      <Routes>
        <Route path="/" element={<CowList />} />
        <Route path="/cow/:id" element={<CowCard />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/create" element={<CreateCowForm />} />
        <Route path="/architecture" element={<ArchitectureView />} />
      </Routes>

      <footer style={{ 
        textAlign: 'center', 
        padding: '2rem', 
        color: 'var(--gray-600)',
        fontSize: '0.875rem',
        marginTop: '4rem'
      }}>
        <div style={{ marginBottom: '0.5rem' }}>
          <strong>Event Sourcing + CQRS + DuckDB Analytics</strong>
        </div>
        <div>
          Events → Bronze → Silver → Gold (Delta) → DuckDB (queries Gold directly, 10-50ms) → FastAPI
        </div>
      </footer>
    </div>
  );
}

export default App;
