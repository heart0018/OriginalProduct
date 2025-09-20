// src/App.tsx
import { Route, BrowserRouter as Router, Routes } from "react-router-dom";
import AuthPage from "./pages/AuthPage";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<AuthPage />} />

      </Routes>
    </Router>
  );
}

export default App;
