package actWeight;

import javax.swing.JProgressBar;

import java.awt.event.ActionEvent;

import javax.swing.JFrame;

public class ProgressBar extends JFrame{

	JProgressBar pbar;
	public ProgressBar() {
		// initialize Progress Bar
		pbar = new JProgressBar();
		pbar.setBounds(50,50,200,30);
		pbar.setValue(0);
		pbar.setStringPainted(true);		
		// add to JPanel
		this.add(pbar);
		this.setSize(400,200);
		this.setLayout(null);
	}
	
	
	public void update(int percent){								
		pbar.setValue(percent);			
	}	
}
