import { render, screen } from '@testing-library/react';
import App from './App';

test('renders SkyStream flight control header', () => {
  render(<App />);
  const brandElement = screen.getByText(/SkyStream/i);
  expect(brandElement).toBeInTheDocument();
});
