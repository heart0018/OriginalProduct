// src/App.tsx
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import SwipePage from './pages/SwipePage';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/swipe" element={<SwipePage />} />
      </Routes>
    </Router>
  );
}

export default App;
