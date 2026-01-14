import { AppLayout, ContentLayout, TopNavigation, Flashbar } from '@cloudscape-design/components'
import { useState, useEffect } from 'react'
import { AppProvider, useAppContext } from './contexts/AppContext'
import ConfigurationForm from './components/ConfigurationForm'
import OptimizerControls from './components/OptimizerControls'
import LogViewer from './components/LogViewer'
import SchemaViewer from './components/SchemaViewer'

function AppContent() {
  const { state, dispatch } = useAppContext()
  const [theme, setTheme] = useState('light')

  const toggleTheme = () => {
    const newTheme = theme === 'light' ? 'dark' : 'light'
    setTheme(newTheme)
    document.documentElement.setAttribute('data-theme', newTheme)
  }

  // Auto-dismiss notifications
  useEffect(() => {
    const timeoutIds: NodeJS.Timeout[] = [];
    state.notifications.forEach(notification => {
      if (notification.autoDismiss) {
        const timeoutId = setTimeout(() => {
          dispatch({ type: 'REMOVE_NOTIFICATION', payload: notification.id })
        }, 5000)
        timeoutIds.push(timeoutId);
      }
    })
    return () => {
      timeoutIds.forEach(id => clearTimeout(id));
    };
  }, [state.notifications, dispatch])

  return (
    <>
      <TopNavigation
        identity={{
          title: "Blueprint Optimizer"
        }}
        utilities={[
          {
            type: "button",
            text: theme === 'light' ? '🌙 Dark' : '☀️ Light',
            onClick: toggleTheme
          }
        ]}
      />
      <AppLayout
        navigationHide
        toolsHide
        content={
          <ContentLayout>
            <Flashbar
              items={state.notifications.map(notification => ({
                id: notification.id,
                type: notification.type,
                content: notification.message,
                dismissible: notification.dismissible,
                onDismiss: () => dispatch({ type: 'REMOVE_NOTIFICATION', payload: notification.id })
              }))}
            />
            <div style={{ marginBottom: '24px' }}>
              <ConfigurationForm />
            </div>
            <div style={{ marginBottom: '24px' }}>
              <OptimizerControls />
            </div>
            <div style={{ marginBottom: '24px' }}>
              <LogViewer />
            </div>
            <div style={{ marginBottom: '24px' }}>
              <SchemaViewer />
            </div>
          </ContentLayout>
        }
      />
    </>
  )
}

function App() {
  return (
    <AppProvider>
      <AppContent />
    </AppProvider>
  )
}

export default App