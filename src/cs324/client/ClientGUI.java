
package cs324.client;

import cs324.common.JobRequest;
import cs324.common.JobType;
import cs324.grpc.JobProgress;
import cs324.grpc.WorkerStats;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ClientGUI extends JFrame {

    private static final Color BACKGROUND = new Color(244, 247, 251);
    private static final Color NAVY = new Color(22, 38, 62);
    private static final Color NAVY_LIGHT = new Color(39, 60, 90);
    private static final Color BUTTON = new Color(28, 107, 181);
    private static final Color BUTTON_HOVER = new Color(22, 88, 154);
    private static final Color WHITE = Color.WHITE;
    private static final Color TEXT = new Color(41, 55, 73);
    private static final Color MUTED = new Color(109, 123, 142);
    private static final Color BORDER = new Color(219, 226, 236);
    private static final Color SUCCESS = new Color(39, 153, 108);
    private static final Color ERROR = new Color(194, 62, 70);

    private JComboBox<JobType> jobTypeBox;
    private JTextField numbersField;
    private JTextField startField;
    private JTextField endField;
    private JPanel inputCards;
    private CardLayout cardLayout;
    private JTextArea resultArea;
    private JLabel coordinatorLabel;
    private JLabel statusLabel;
    private JLabel jobTypeResultLabel;
    private JTextArea activityArea;
    private JButton loadCSVButton;
    private JButton submitButton;
    private JPanel progressList;
    private final Map<String, JProgressBar> progressBars = new ConcurrentHashMap<>();
    private final Map<String, JobProgress> latestProgress = new ConcurrentHashMap<>();

    private final ExecutorService clientPool = Executors.newCachedThreadPool();

    public ClientGUI() {
        setTitle("DistriLab | Distributed Computing Client");
        setSize(980, 680);
        setMinimumSize(new Dimension(890, 610));
        setLocationRelativeTo(null);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JPanel root = new JPanel(new BorderLayout());
        root.setBackground(BACKGROUND);
        setContentPane(root);
        root.add(createHeader(), BorderLayout.NORTH);

        JPanel content = new JPanel(new BorderLayout(0, 18));
        content.setOpaque(false);
        content.setBorder(new EmptyBorder(22, 26, 22, 26));

        JPanel topCards = new JPanel(new GridLayout(1, 2, 18, 0));
        topCards.setOpaque(false);
        topCards.add(createJobCard());
        topCards.add(createResultCard());

        content.add(topCards, BorderLayout.NORTH);
        content.add(createActivityCard(), BorderLayout.CENTER);

        root.add(content, BorderLayout.CENTER);
        root.add(createStatusBar(), BorderLayout.SOUTH);
        updateInputPanel();

        // Add shortcut Ctrl+Enter to submit jobs quickly
        getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
                .put(KeyStroke.getKeyStroke("control ENTER"), "submitJobShortcut");
        getRootPane().getActionMap().put("submitJobShortcut", new AbstractAction() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent e) {
                submitButton.doClick();
            }
        });

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                clientPool.shutdownNow();
            }
        });
    }

    // ==========================================
    // HEADER
    // ==========================================

    private JPanel createHeader() {
        JPanel header = new GradientPanel();
        header.setLayout(new BorderLayout());
        header.setBorder(new EmptyBorder(20, 28, 20, 28));

        JPanel titles = new JPanel();
        titles.setOpaque(false);
        titles.setLayout(new BoxLayout(titles, BoxLayout.Y_AXIS));

        JLabel brand = new JLabel("DistriLab");
        brand.setFont(new Font("Segoe UI", Font.BOLD, 27));
        brand.setForeground(WHITE);
        brand.setAlignmentX(Component.LEFT_ALIGNMENT);

        JLabel subtitle = new JLabel("Distributed Computing Client");
        subtitle.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        subtitle.setForeground(new Color(199, 213, 231));
        subtitle.setAlignmentX(Component.LEFT_ALIGNMENT);

        titles.add(brand);
        titles.add(Box.createVerticalStrut(3));
        titles.add(subtitle);
        header.add(titles, BorderLayout.WEST);

        JLabel badge = new JLabel("gRPC CLIENT");
        badge.setOpaque(true);
        badge.setBackground(BUTTON);
        badge.setForeground(WHITE);
        badge.setFont(new Font("Segoe UI", Font.BOLD, 11));
        badge.setBorder(new EmptyBorder(9, 13, 9, 13));
        header.add(badge, BorderLayout.EAST);

        return header;
    }

    // ==========================================
    // JOB CONFIGURATION
    // ==========================================

    private JPanel createJobCard() {
        JPanel card = createCard();
        card.setLayout(new BoxLayout(card, BoxLayout.Y_AXIS));

        card.add(sectionTitle("Job Configuration"));
        card.add(Box.createVerticalStrut(4));
        card.add(sectionSubtitle("Select a calculation and enter your data."));
        card.add(Box.createVerticalStrut(19));
        card.add(fieldLabel("JOB TYPE"));
        card.add(Box.createVerticalStrut(7));

        jobTypeBox = new JComboBox<>(JobType.values());
        jobTypeBox.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        jobTypeBox.setBackground(WHITE);
        jobTypeBox.setForeground(TEXT);
        jobTypeBox.setAlignmentX(Component.LEFT_ALIGNMENT);
        jobTypeBox.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, 38)
        );
        jobTypeBox.setToolTipText("Select the distributed calculation task type");

        card.add(jobTypeBox);
        card.add(Box.createVerticalStrut(16));

        cardLayout = new CardLayout();
        inputCards = new JPanel(cardLayout);
        inputCards.setOpaque(false);
        inputCards.setAlignmentX(Component.LEFT_ALIGNMENT);
        inputCards.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, 95)
        );

        inputCards.add(createListInputPanel(), "LIST");
        inputCards.add(createRangeInputPanel(), "RANGE");

        card.add(inputCards);
        card.add(Box.createVerticalStrut(15));

        JPanel buttons = new JPanel(new GridLayout(1, 2, 10, 0));
        buttons.setOpaque(false);
        buttons.setAlignmentX(Component.LEFT_ALIGNMENT);
        buttons.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, 42)
        );

        loadCSVButton = createButton("Load CSV");
        submitButton = createButton("Submit Job");

        buttons.add(loadCSVButton);
        buttons.add(submitButton);
        card.add(buttons);

        jobTypeBox.addActionListener(e -> updateInputPanel());
        loadCSVButton.addActionListener(e -> loadCSV());
        submitButton.addActionListener(e -> submitJob());

        return card;
    }

    // ==========================================
    // NUMBER INPUT
    // ==========================================

    private JPanel createListInputPanel() {
        JPanel panel = new JPanel();
        panel.setOpaque(false);
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        panel.add(fieldLabel("NUMBERS"));
        panel.add(Box.createVerticalStrut(7));

        numbersField = createTextField();
        numbersField.setToolTipText(
                "Example: 12, 45, 1000000000000000000000000"
        );

        panel.add(numbersField);
        return panel;
    }

    // ==========================================
    // PRIME RANGE INPUT
    // ==========================================

    private JPanel createRangeInputPanel() {
        JPanel panel = new JPanel();
        panel.setOpaque(false);
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        panel.add(fieldLabel("PRIME RANGE (START / END)"));
        panel.add(Box.createVerticalStrut(7));

        JPanel fields = new JPanel(new GridLayout(1, 2, 10, 0));
        fields.setOpaque(false);

        startField = createTextField();
        endField = createTextField();

        startField.setToolTipText("Start of range");
        endField.setToolTipText("End of range");

        fields.add(startField);
        fields.add(endField);
        panel.add(fields);

        return panel;
    }

    // ==========================================
    // RESULT CARD
    // ==========================================

    private JPanel createResultCard() {
        JPanel card = createCard();
        card.setLayout(new BoxLayout(card, BoxLayout.Y_AXIS));

        card.add(sectionTitle("Latest Result"));
        card.add(Box.createVerticalStrut(4));
        card.add(sectionSubtitle(
                "Output from the distributed worker network."
        ));
        card.add(Box.createVerticalStrut(24));

        jobTypeResultLabel = new JLabel("NO JOB SUBMITTED");
        jobTypeResultLabel.setFont(
                new Font("Segoe UI", Font.BOLD, 11)
        );
        jobTypeResultLabel.setForeground(MUTED);
        jobTypeResultLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        card.add(jobTypeResultLabel);
        card.add(Box.createVerticalStrut(12));

        // Long results stay visible and can be copied.
        resultArea = new JTextArea("—");
        resultArea.setEditable(false);
        resultArea.setLineWrap(true);
        resultArea.setWrapStyleWord(false);
        resultArea.setFont(
                new Font("Segoe UI", Font.BOLD, 24)
        );
        resultArea.setForeground(NAVY);
        resultArea.setBackground(new Color(247, 249, 252));
        resultArea.setBorder(new EmptyBorder(12, 12, 12, 12));

        JScrollPane resultScroll = new JScrollPane(resultArea);
        resultScroll.setBorder(
                BorderFactory.createLineBorder(BORDER)
        );
        resultScroll.setAlignmentX(Component.CENTER_ALIGNMENT);
        resultScroll.setPreferredSize(new Dimension(360, 100));
        resultScroll.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, 100)
        );

        card.add(resultScroll);
        card.add(Box.createVerticalStrut(16));

        coordinatorLabel = new JLabel("Coordinator: —");
        coordinatorLabel.setFont(
                new Font("Segoe UI", Font.PLAIN, 13)
        );
        coordinatorLabel.setForeground(MUTED);
        coordinatorLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

        card.add(coordinatorLabel);
        card.add(Box.createVerticalStrut(12));
        JButton clearResultButton = createButton("Clear Result");
        clearResultButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        clearResultButton.setMaximumSize(new Dimension(140, 32));
        clearResultButton.addActionListener(e -> {
            resultArea.setText("—");
            jobTypeResultLabel.setText("NO JOB SUBMITTED");
            coordinatorLabel.setText("Coordinator: —");
        });
        card.add(clearResultButton);
        return card;
    }

    // ==========================================
    // ACTIVITY LOG
    // ==========================================

    private JPanel createActivityCard() {
        JPanel card = createCard();
        card.setLayout(new BorderLayout(0, 12));

        JPanel heading = new JPanel(new BorderLayout());
        heading.setOpaque(false);

        JPanel titles = new JPanel();
        titles.setOpaque(false);
        titles.setLayout(new BoxLayout(titles, BoxLayout.Y_AXIS));

        titles.add(sectionTitle("Task Activity"));
        titles.add(Box.createVerticalStrut(3));
        titles.add(sectionSubtitle(
                "History of jobs submitted by this client."
        ));

        heading.add(titles, BorderLayout.WEST);

        JButton clearButton = createButton("Clear");
        clearButton.setPreferredSize(new Dimension(95, 36));
        clearButton.addActionListener(
                e -> activityArea.setText("")
        );

        heading.add(clearButton, BorderLayout.EAST);
        // Click any job's progress bar for actual per-worker completed tasks.
        progressList = new JPanel();
        progressList.setBackground(WHITE);
        progressList.setLayout(new BoxLayout(progressList, BoxLayout.Y_AXIS));
        JScrollPane progressScroll = new JScrollPane(progressList);
        progressScroll.setPreferredSize(new Dimension(300, 110));
        progressScroll.setBorder(BorderFactory.createLineBorder(BORDER));

        JPanel activityTop = new JPanel(new BorderLayout(0, 9));
        activityTop.setOpaque(false);
        activityTop.add(heading, BorderLayout.NORTH);
        activityTop.add(progressScroll, BorderLayout.CENTER);
        card.add(activityTop, BorderLayout.NORTH);

        activityArea = new JTextArea();
        activityArea.setEditable(false);
        activityArea.setFont(
                new Font("Consolas", Font.PLAIN, 13)
        );
        activityArea.setForeground(TEXT);
        activityArea.setBackground(new Color(247, 249, 252));
        activityArea.setBorder(
                new EmptyBorder(12, 12, 12, 12)
        );

        JScrollPane scroll = new JScrollPane(activityArea);
        scroll.setBorder(
                BorderFactory.createLineBorder(BORDER)
        );

        card.add(scroll, BorderLayout.CENTER);
        return card;
    }

    // ==========================================
    // STATUS BAR
    // ==========================================

    private JPanel createStatusBar() {
        JPanel bar = new JPanel(new BorderLayout());
        bar.setBackground(NAVY);
        bar.setBorder(new EmptyBorder(11, 26, 11, 26));

        statusLabel = new JLabel("● READY");
        statusLabel.setFont(
                new Font("Segoe UI", Font.BOLD, 12)
        );
        statusLabel.setForeground(new Color(116, 221, 176));

        JLabel networkLabel = new JLabel(
                "DistriLab Network  |  gRPC"
        );
        networkLabel.setFont(
                new Font("Segoe UI", Font.PLAIN, 12)
        );
        networkLabel.setForeground(new Color(189, 203, 220));

        bar.add(statusLabel, BorderLayout.WEST);
        bar.add(networkLabel, BorderLayout.EAST);

        return bar;
    }

    // ==========================================
    // SWITCH INPUT PANEL
    // ==========================================

    private void updateInputPanel() {
        JobType type = (JobType) jobTypeBox.getSelectedItem();

        if (type == JobType.PRIMESUM) {
            cardLayout.show(inputCards, "RANGE");
            loadCSVButton.setText("Load Range CSV");
        } else {
            cardLayout.show(inputCards, "LIST");
            loadCSVButton.setText("Load CSV");
        }
    }

    // ==========================================
    // LOAD CSV
    // ==========================================

    private void loadCSV() {
        JFileChooser chooser = new JFileChooser();

        if (chooser.showOpenDialog(this)
                != JFileChooser.APPROVE_OPTION) {
            return;
        }

        try {
            File file = chooser.getSelectedFile();
            List<BigInteger> numbers = readCSVNumbers(file);

            JobType type = (JobType) jobTypeBox.getSelectedItem();

            if (type == JobType.PRIMESUM) {
                if (numbers.size() < 2) {
                    throw new IllegalArgumentException(
                            "PRIMESUM CSV requires a start and end value."
                    );
                }

                startField.setText(numbers.get(0).toString());
                endField.setText(numbers.get(1).toString());

            } else {
                numbersField.setText(
                        numbers.stream()
                                .map(BigInteger::toString)
                                .collect(Collectors.joining(", "))
                );
            }

            updateStatus(
                    "CSV LOADED",
                    new Color(116, 221, 176)
            );

            appendActivity("[CSV] Loaded: " + file.getName());

        } catch (Exception e) {
            showError(e.getMessage());
        }
    }

    private List<BigInteger> readCSVNumbers(File file)
            throws Exception {

        List<BigInteger> numbers = new ArrayList<>();
        boolean firstRow = true;

        for (String rawLine : Files.readAllLines(
                file.toPath(),
                StandardCharsets.UTF_8)) {

            String line = rawLine.replace("\uFEFF", "").trim();

            if (line.isEmpty()) {
                continue;
            }

            String[] pieces = line.split("[,;\\s]+");

            if (firstRow && isHeaderRow(pieces)) {
                firstRow = false;
                continue;
            }

            firstRow = false;

            for (String piece : pieces) {
                String value = piece.replace("\"", "").trim();

                if (!value.isEmpty()) {
                    try {
                        numbers.add(new BigInteger(value));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                                "Invalid number in CSV: " + piece
                        );
                    }
                }
            }
        }

        if (numbers.isEmpty()) {
            throw new IllegalArgumentException(
                    "CSV file contains no numbers."
            );
        }

        return numbers;
    }

    private boolean isHeaderRow(String[] pieces) {
        for (String piece : pieces) {
            String word = piece.replace("\"", "").trim();

            if (!word.matches(
                    "(?i)start|end|number|numbers|value|values|integer|integers"
            )) {
                return false;
            }
        }

        return true;
    }

    // ==========================================
    // BUILD JOB
    // ==========================================

    private JobRequest buildJob() {
        JobType type = (JobType) jobTypeBox.getSelectedItem();

        if (type == JobType.PRIMESUM) {
            String startText = startField.getText().trim();
            String endText = endField.getText().trim();

            if (startText.isEmpty() || endText.isEmpty()) {
                throw new IllegalArgumentException(
                        "Please enter both start and end values."
                );
            }

            BigInteger start = parseInteger(startText);
            BigInteger end = parseInteger(endText);

            if (start.compareTo(end) > 0) {
                throw new IllegalArgumentException(
                        "Start value cannot be greater than end value."
                );
            }

            return new JobRequest(
                    JobType.PRIMESUM,
                    start,
                    end
            );
        }

        String input = numbersField.getText().trim();

        if (input.isEmpty()) {
            throw new IllegalArgumentException(
                    "Please enter at least one number."
            );
        }

        List<BigInteger> numbers = new ArrayList<>();

        for (String piece : input.split("[,\\s]+")) {
            if (!piece.isEmpty()) {
                numbers.add(parseInteger(piece));
            }
        }

        return new JobRequest(type, numbers);
    }

    private BigInteger parseInteger(String text) {
        String cleanText = text.trim();
        if (cleanText.isEmpty()) {
            throw new IllegalArgumentException("Input number cannot be empty.");
        }
        try {
            return new BigInteger(cleanText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid integer format: \"" + cleanText + "\". Please use valid numbers only."
            );
        }
    }

    // ==========================================
    // SUBMIT JOB
    // ==========================================

    private void submitJob() {
        final JobRequest job;
        try {
            job = buildJob();
        } catch (Exception e) {
            showError("Invalid input.\n" + e.getMessage());
            return;
        }

        addProgressBar(job);
        appendActivity("[SUBMITTED] " + job.getJobType() + " | " + shortJobId(job.getJobId()));
        updateStatus("PROCESSING", new Color(255, 207, 105));

        clientPool.submit(() -> {
            try {
                GrpcClientConnection.stream(job, update -> {
                    latestProgress.put(job.getJobId(), update);
                    SwingUtilities.invokeLater(() -> {
                        JProgressBar bar = progressBars.get(job.getJobId());
                        if (bar != null) bar.setValue(update.getPercent());
                        coordinatorLabel.setText("Coordinator: Worker " + update.getCoordinatorId());
                        if (update.getFinished()) {
                            jobTypeResultLabel.setText(job.getJobType() + " RESULT");
                            resultArea.setForeground(NAVY);
                            resultArea.setText(update.getResult());
                            resultArea.setCaretPosition(0);
                            updateStatus("COMPLETED", new Color(116, 221, 176));
                        }
                    });
                    if (update.getFinished()) {
                        appendActivity("[COMPLETED] " + job.getJobType()
                                + " → " + update.getResult()
                                + " | Coordinator W" + update.getCoordinatorId());
                    }
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    resultArea.setText("ERROR");
                    resultArea.setForeground(ERROR);
                    updateStatus("FAILED", new Color(255, 133, 145));
                });
                appendActivity("[FAILED] " + job.getJobType() + " → " + e.getMessage());
            }
        });
    }

    private void addProgressBar(JobRequest job) {
        JProgressBar bar = new JProgressBar(0, 100);
        bar.setStringPainted(true);
        bar.setValue(0);
        bar.setForeground(BUTTON);
        bar.setBackground(new Color(235, 240, 247));
        bar.setPreferredSize(new Dimension(300, 26));
        bar.setMaximumSize(new Dimension(Integer.MAX_VALUE, 26));
        bar.setToolTipText("Click to see worker-level breakdown");
        bar.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        bar.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override
            public void mouseClicked(java.awt.event.MouseEvent e) {
                JobProgress update = latestProgress.get(job.getJobId());
                StringBuilder details = new StringBuilder();
                details.append(job.getJobType()).append(" | ").append(shortJobId(job.getJobId())).append("\n");
                if (update == null) {
                    details.append("Waiting for workers...");
                } else {
                    details.append(update.getCompleted()).append("/")
                            .append(update.getTotal()).append(" tasks completed\n\n");
                    for (WorkerStats stats : update.getWorkersList()) {
                        details.append("Worker ").append(stats.getWorkerId())
                                .append(": ").append(stats.getCompletedTasks())
                                .append(" completed, ").append(stats.getAssignedTasks())
                                .append(" assigned\n");
                    }
                }
                JOptionPane.showMessageDialog(ClientGUI.this, details.toString(),
                        "Worker Progress", JOptionPane.INFORMATION_MESSAGE);
            }
        });
        progressBars.put(job.getJobId(), bar);
        JPanel item = new JPanel(new BorderLayout(8, 0));
        item.setOpaque(false);
        item.setBorder(new EmptyBorder(5, 6, 5, 6));
        JLabel title = new JLabel(job.getJobType() + " • " + shortJobId(job.getJobId()));
        title.setFont(new Font("Segoe UI", Font.BOLD, 11));
        title.setForeground(TEXT);
        item.add(title, BorderLayout.WEST);
        item.add(bar, BorderLayout.CENTER);
        progressList.add(item);
        progressList.revalidate();
        progressList.repaint();
    }

    // ==========================================
    // ACTIVITY AND STATUS
    // ==========================================

    private void appendActivity(String text) {
        SwingUtilities.invokeLater(() -> {
            activityArea.append(text + "\n");

            activityArea.setCaretPosition(
                    activityArea.getDocument().getLength()
            );
        });
    }

    private void updateStatus(String text, Color colour) {
        SwingUtilities.invokeLater(() -> {
            statusLabel.setText("● " + text);
            statusLabel.setForeground(colour);
        });
    }

    private void showError(String message) {
        JOptionPane.showMessageDialog(
                this,
                message,
                "DistriLab Error",
                JOptionPane.ERROR_MESSAGE
        );
    }

    private String shortJobId(String jobId) {
        return jobId == null || jobId.length() <= 8
                ? jobId : jobId.substring(0, 8);
    }

    // ==========================================
    // UI STYLING
    // ==========================================

    private JPanel createCard() {
        JPanel panel = new JPanel();
        panel.setBackground(WHITE);

        panel.setBorder(
                BorderFactory.createCompoundBorder(
                        BorderFactory.createLineBorder(BORDER),
                        new EmptyBorder(20, 21, 20, 21)
                )
        );

        return panel;
    }

    private JLabel sectionTitle(String text) {
        JLabel label = new JLabel(text);
        label.setFont(
                new Font("Segoe UI", Font.BOLD, 18)
        );
        label.setForeground(NAVY);
        label.setAlignmentX(Component.LEFT_ALIGNMENT);
        return label;
    }

    private JLabel sectionSubtitle(String text) {
        JLabel label = new JLabel(text);
        label.setFont(
                new Font("Segoe UI", Font.PLAIN, 12)
        );
        label.setForeground(MUTED);
        label.setAlignmentX(Component.LEFT_ALIGNMENT);
        return label;
    }

    private JLabel fieldLabel(String text) {
        JLabel label = new JLabel(text);
        label.setFont(
                new Font("Segoe UI", Font.BOLD, 11)
        );
        label.setForeground(MUTED);
        label.setAlignmentX(Component.LEFT_ALIGNMENT);
        return label;
    }

    private JTextField createTextField() {
        JTextField field = new JTextField();

        field.setFont(
                new Font("Segoe UI", Font.PLAIN, 14)
        );
        field.setForeground(TEXT);
        field.setBackground(new Color(248, 250, 253));

        field.setBorder(
                BorderFactory.createCompoundBorder(
                        BorderFactory.createLineBorder(BORDER),
                        new EmptyBorder(9, 11, 9, 11)
                )
        );

        field.setAlignmentX(Component.LEFT_ALIGNMENT);
        field.setMaximumSize(
                new Dimension(Integer.MAX_VALUE, 40)
        );

        return field;
    }

    // All buttons use the same blue colour and hover style.
    private JButton createButton(String text) {
        JButton button = new JButton(text) {
            @Override
            protected void paintComponent(Graphics g) {
                Graphics2D g2 = (Graphics2D) g.create();

                g2.setRenderingHint(
                        RenderingHints.KEY_ANTIALIASING,
                        RenderingHints.VALUE_ANTIALIAS_ON
                );

                g2.setColor(
                        getModel().isRollover()
                                ? BUTTON_HOVER
                                : BUTTON
                );

                g2.fillRoundRect(
                        0,
                        0,
                        getWidth(),
                        getHeight(),
                        10,
                        10
                );

                g2.dispose();
                super.paintComponent(g);
            }
        };

        button.setFont(
                new Font("Segoe UI", Font.BOLD, 13)
        );
        button.setForeground(WHITE);
        button.setFocusPainted(false);
        button.setContentAreaFilled(false);
        button.setBorderPainted(false);
        button.setOpaque(false);
        button.setRolloverEnabled(true);
        button.setCursor(
                Cursor.getPredefinedCursor(Cursor.HAND_CURSOR)
        );

        return button;
    }

    // ==========================================
    // GRADIENT HEADER
    // ==========================================

    private static class GradientPanel extends JPanel {
        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();

            g2.setPaint(
                    new GradientPaint(
                            0,
                            0,
                            NAVY,
                            getWidth(),
                            0,
                            NAVY_LIGHT
                    )
            );

            g2.fillRect(
                    0,
                    0,
                    getWidth(),
                    getHeight()
            );

            g2.dispose();
            super.paintComponent(g);
        }

        @Override
        public boolean isOpaque() {
            return false;
        }
    }

    // ==========================================
    // MAIN
    // ==========================================

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(
                    UIManager.getSystemLookAndFeelClassName()
            );
        } catch (Exception ignored) {
        }

        SwingUtilities.invokeLater(() -> {
            ClientGUI gui = new ClientGUI();
            gui.setVisible(true);
        });
    }
}
